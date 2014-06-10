var app = app || {};

(function () {
	'use strict';

	app.Job = Backbone.Model.extend({
		defaults: {
			name : "",
			executor : "",
			executor_flag : "",
			owner : "",
		},
		urlRoot: '/job',
	});
})();