var app = app || {};

(function () {
	'use strict';

	var Jobs = Backbone.Collection.extend({
		// Reference to this collection's model.
		model: app.Job,
		url : "/job/list",
		comparator: function (job) {
			return job.get('id');
		}
	});

	app.jobs = new Jobs();
})();