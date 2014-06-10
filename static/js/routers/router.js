/*global Backbone */
var app = app || {};

(function () {
	'use strict';
	app.AppRouter = Backbone.Router.extend({
	    routes: {
	        "*Page" : "goPage",
	    },
	    goPage: function(id) {
	    	if (id == null) id = 'job';
		    app.appView.render(id);
	    },
	});

	app.appView = new app.AppView();
	app.router = new app.AppRouter;
	Backbone.history.start();
})();