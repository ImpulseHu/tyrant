/*global Backbone */
var app = app || {};

(function () {
	'use strict';
	app.AppRouter = Backbone.Router.extend({
	    routes: {
	        'job' : 'job',
	        'status' : 'status',
	        'executor' : 'executor',
	        '*action' : 'defaultRoute'
	    },
	    executor : function () {
	    	app.appView.render('executor');
	    },
	    job : function () {
	    	app.appView.render('job');
	    },
	    status: function () {
	    	app.appView.render('status');
	    },
	    defaultRoute : function() {
	    	app.appView.render('job');
	    },
	});
})();