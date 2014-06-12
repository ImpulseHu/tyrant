var app = app || {};

(function () {
	'use strict';

	app.Task = Backbone.Model.extend({
		defaults: {
			id : "",
			job_id : "",
			start_ts : "",
			update_ts : "",
			status : "",
			message : "",
			url : "",
		},
		urlRoot: '/task',
	});
})();