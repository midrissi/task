# Example
* Start the listener
```js
var Task = require('task');
var Redis = require('ioredis');
var client = new Redis();

Task.listen(listener);
```
* Create the worker
```js
exports.name = 'billig:anually';

exports.execute = function (task) {
	console.log('********************* Here we can do some work! *********************');
	return task.save();
};

exports.filename = __filename;
```
* Create a task based on the worker above
```js
var Task = require('task');
var Redis = require('ioredis');
var client = new Redis();
var task = new Task(redis);

task.data({
    key: 'value'
});
task.id('someid');
task.ttl(3);
task.module(worker);
task.save().catch(function () {
	console.log(arguments);
});
```
