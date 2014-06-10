var app = app || {};

(function () {
	'use strict';

	app.Task = Backbone.Model.extend({
		defaults: {
			job_id : "",
			start_ts : "",
			finish_ts : "",
			status : "",
			stdout : "",
			stderr : "",
		},
		urlRoot: '/task',
	});
})();