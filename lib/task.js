var Q = require('q');
var Redis = require('ioredis');
var path = require('path');

function Task(client) {
  if (!client) {
    throw 'The client is mondatory.';
    return;
  }

  this.client = client;
  this._loaded = false;
  this._ttl = Task.MONTH;
}

Task.SECOND = 1;
Task.MINUTE = 60;
Task.HOUR = 3600;
Task.DAY = 86400;
Task.WEEK = 604800;
Task.MONTH = 2592000;
Task.YEAR = 31536000;
Task.PLOCK = 'lock';
Task.PVALUE = 'value';
Task.PEXPIRE = 'expire';
Task.PREFIX = 'resources:';

function getFullKey(key, type) {
  var prefix = null;

  switch (type) {
    case 'e':
      prefix = Task.PEXPIRE;
      break;
    case 'l':
      prefix = Task.PLOCK;
      break;
    case 'v':
      prefix = Task.PVALUE;
      break;
  }

  if (prefix) {
    return Task.PREFIX + key + ':' + prefix;
  }
}

/**
 * Listen for the expire events
 */
Task.listen = function(client) {
  var regex = new RegExp(
    '__keyspace.*:' +
    Task.PREFIX + '(.*):(.*):' +
    Task.PEXPIRE
  );
  client.psubscribe(
    '__keyspace*:' + Task.PREFIX + '*:' + Task.PEXPIRE,
    function(err, count) {}
  );

  client.on('pmessage', function(pattern, channel, message) {
    if (regex.test(channel) && message === 'expired') {
      var exec = regex.exec(channel);

      Task
        .load(new Redis(client.options), exec[1], exec[2])
        .then(function(task) {
          var res = task.execute();

          if (Q.isPromise(res)) {
            return res.then(function() {
              task.unlock();
            });
          }

          task.unlock();
        })
        .catch(function() {
          console.error(arguments);
        });
    }
  });
};

/**
 * @param  {Module}
 * @param  {String}
 * @return {Promise}
 */
Task.load = function(client, modName, id) {
  return Q.Promise(function(resolve, reject, notify) {
    if (!client) {
      return reject('The client is mondatory.');
      return;
    }

    var pipeline = client.pipeline();

    pipeline.setnx(
      getFullKey(modName + ':' + id, 'l'),
      "1"
    );

    pipeline.get(
      getFullKey(modName + ':' + id, 'v')
    );

    pipeline.exec(function(err, results) {
      if (err) {
        return reject(err);
      }

      if (results[0][0]) {
        return reject(results[0][0]);
      }

      if (results[1][0]) {
        return reject(results[1][0]);
      }

      if (!results[0][1]) {
        return reject('Task locked');
      }

      var task = Task.resolve(client, results[1][1]);
      task.id(id);
      return resolve(task);
    });
  });
};

/**
 * @param  {Object}
 * @return {Task}
 */
Task.resolve = function(client, data) {
  var res = new Task(client);

  if (typeof data === 'string') {
    data = JSON.parse(data);
  }

  res.data(data.data);
  res.ttl(data.ttl);
  res.module(
    require(
      path.resolve(module.filename, data.module)
    )
  );

  return res;
};

/**
 * Sets or get the data of the task
 * @param  {Object} data 
 * @return {Task}
 */
Task.prototype.data = function(data) {
  if (typeof data !== 'undefined') {
    this._data = data;
    return this;
  }

  return this._data;
};

/**
 * Sets or gets the task identifier
 * @param  {String} id 
 * @return {Task}
 */
Task.prototype.id = function(id) {
  if (typeof id !== 'undefined') {
    this._id = id;
    return this;
  }

  return this._id;
};

/**
 * Sets or gets the task identifier
 * @param  {String} ttl 
 * @return {Task}
 */
Task.prototype.ttl = function(ttl) {
  if (typeof ttl !== 'undefined') {
    this._ttl = ttl;
    return this;
  }

  return this._ttl;
};

/**
 * Sets or gets the task module
 * @param  {Module} module 
 * @return {Task}
 */
Task.prototype.module = function(module) {
  if (typeof module !== 'undefined' && typeof module.execute === 'function') {

    this._module = module;
    return this;
  }

  return this._module;
};

/**
 * Execute the task
 * @return {Task}
 */
Task.prototype.execute = function() {
  var mod = this.module();

  if (mod) {
    return mod.execute(this);
  }
};

/**
 * Locks the current task
 * Usefull when you want to execute the task once
 * @return {Promise}
 */
Task.prototype.lock = function() {
  var mod = this.module();
  if (mod) {
    var that = this;

    return Q.Promise(function(resolve, reject, notify) {
      that.client.setnx(
        getFullKey(mod.name + ':' + that.id(), 'l'),
        "1",
        function(err, res) {
          if (err) {
            return reject(err);
          }

          resolve(res);
        }
      );
    });
  }
};

/**
 * Unlocks the current task
 * Usefull when you want to execute the task once
 * @return {Promise}
 */
Task.prototype.unlock = function() {
  var mod = this.module();
  if (mod) {
    var that = this;

    return Q.Promise(function(resolve, reject, notify) {
      that.client.del(
        getFullKey(mod.name + ':' + that.id(), 'l'),
        function(err, res) {
          if (err) {
            return reject(err);
          }

          resolve(res);
        }
      );
    });
  }
};

/**
 * Save the current task to be accessible fom everywhere
 * @param  {Function}
 * @return {Promise}
 */
Task.prototype.save = function() {
  var that = this;

  return Q.Promise(function(resolve, reject, notify) {
    var pipeline = that.client.pipeline();
    var mod = that.module();
    var data = that.data();

    if (!mod) {
      return reject('Undefined task module!');
    }

    pipeline.set(
      getFullKey(mod.name + ':' + that.id(), 'v'), JSON.stringify({
        data: that.data(),
        ttl: that.ttl(),
        module: path.relative(module.filename, mod.filename)
      })
    );

    pipeline.set(
      getFullKey(mod.name + ':' + that.id(), 'e'),
      1
    );

    pipeline.expire(
      getFullKey(mod.name + ':' + that.id(), 'e'),
      that.ttl()
    );

    pipeline.exec(function(err, resuls) {
      if (err) {
        return reject(err);
      }

      resolve(resuls);
    });
  });
};

module.exports = Task;