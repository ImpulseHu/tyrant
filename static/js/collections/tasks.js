var app = app || {};

(function () {
	'use strict';

	var Tasks = Backbone.Collection.extend({
		model: app.Task,
		url : "/task/list"
	});

	app.tasks = new Tasks();
})();