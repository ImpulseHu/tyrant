var app = app || {};
$(function () {
	'use strict';
	app.appView = new app.AppView();
	Backbone.history.start();
});
