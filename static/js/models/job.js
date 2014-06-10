var app = app || {};

(function () {
	'use strict';

	app.Job = Backbone.Model.extend({
		defaults: {
			name : "",
			executor : "",
			last_status : "",
			executor_flags : "",
			uris : "",
			owner : "",
		},
		urlRoot: '/job',
	});
})();