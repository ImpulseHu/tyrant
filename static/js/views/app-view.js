var app = app || {};
(function ($) {
	'use strict';

	app.HeaderView = Backbone.View.extend({
		template : _.template($("#header-template").html()),

		render: function() {
          this.$el.html(this.template());
          return this;
        },

        setActive: function(id) {
			_.each(this.$el.find('li'), function(li) {
				$(li).removeClass('active');
			});
			this.$el.find('.' + id).addClass('active');  	
        }
	});


	app.JobEditView = Backbone.View.extend({
		template : _.template($("#job-edit-template").html()),

		render: function() {
          this.$el.html(this.template(this.model.toJSON()));
          return this;
        },

        getObject: function() {
        	alert(this.$el.find(".executor-flag-input").val())
        	return {
				name : this.$el.find(".name-input").val(),
	    		executor : this.$el.find(".executor-input").val(),
	    	    executor_flags : this.$el.find(".executor-flag-input").val(),
				uris : this.$el.find('.uris-input').val(),
				schedule : this.$el.find('.schedule-input').val(),
	    	    owner : this.$el.find(".owner-input").val()
			}
        }
	});


	app.JobNewView = Backbone.View.extend({

		tagName:  'div',
			
		events: {
			'click .btn-save': 'onSaveClick',
		},

		initialize: function () {
			this.$form = new app.JobEditView({model: this.model});
		},

		onSaveClick : function () {
			var job = new app.Job();
			job.save(this.$form.getObject() , {
				success : function(j, e) {
					j.set('id', e.data.id);
					j.set('create_ts', e.data.create_ts);
					app.jobs.add(j);
				},
				error: function(j, e) {
					alert(e.responseJSON.msg);
				}
			});
		},

		render: function() {
          this.$el.html(this.$form.render().el);
          return this;
        }
	});

	app.ModalContent = Backbone.View.extend({

		initialize: function () {
			this.$form = new app.JobEditView({model: this.model});
	        this.bind("ok", this.onOkClicked);
	    },

	    onOkClicked: function (modal) {
	    	this.model.save(
	    		this.$form.getObject()
	    	);
	        //modal.preventClose();
	    },

        render: function() {
          this.$el.html(this.$form.render().el);
          this.$el.find(".btn-save").hide();
          return this;
        }
    });

	app.TaskView = Backbone.View.extend({
		tagName:  'tr',
		template: _.template($('#task-item-template').html()),
		render: function () {
			this.$el.html(this.template(this.model.toJSON()));
			return this;
		},
	});

	app.JobView = Backbone.View.extend({

		tagName:  'tr',

		template: _.template($('#job-item-template').html()),

		events: {
			'click .edit-btn': 'onEditClick',
			'click .remove-btn': 'onRemoveClick',
			'click .force-run-btn' : 'onRunClick',
		},

		initialize: function () {
			this.listenTo(this.model, 'change', this.render);
		},

		onRunClick: function () {
			$.ajax({
				url: '/job/run/' + this.model.get('id'),
				type : 'POST',
				error: function(e) {
					console.log(e);
				},
				success: function(result) {
					alert("OK" + " " + result);
				}
			});
		},

		onRemoveClick: function () {
			var that = this;
			this.model.destroy({success: function(m, resp) {
				app.jobs.remove(m);
				that.remove();
			}});
		},

		onEditClick: function () {
			var editView = new Backbone.BootstrapModal({
				animate: true,
          		content: new app.ModalContent({model:this.model})
			});
			editView.open();
		},

		render: function () {
			this.$el.html(this.template(this.model.toJSON()));
			return this;
		},
	});

	app.AppView = Backbone.View.extend({

		el: '#tyrantapp',

		events: {
		},

		initialize: function () {
			this.listenTo(app.jobs, 'add', this.addOneJob);
			this.listenTo(app.tasks, 'add', this.addOneTask);
			this.header = new app.HeaderView();

			app.router = new app.AppRouter();
		},

		render: function (page) {
			if (page == 'job') {
				this.jobPage();
			} else if (page == 'status') {
				this.statusPage();
			}
		},

		jobPage : function () {
			var list_tmpl = _.template($("#job-view-template").html());
			var newJobView = new app.JobNewView({model : new app.Job});
			// clear page
			this.$el.html('');
			// set header
			this.$el.append(this.header.render().el);
			// set job list
			this.$el.append(list_tmpl());
 			this.$list = $('#job-list');
			// set new job form
			this.$el.find('#new-job-form').html(newJobView.render().el);
			// set header active
			this.header.setActive('job');
			// load job list
 			app.jobs.fetch({success: function(d, e) {
				_.each(e.data, function(o){ 
					app.jobs.add(o); 
				});
			}, reset: true});
		},

		statusPage : function() {
			var status_list_tmpl = _.template($("#status-view-template").html())
			this.$el.html('');
			this.$el.append(this.header.render().el);
			this.$el.append(status_list_tmpl());
			this.$list = $('.running');
			this.header.setActive('status');
			app.tasks.fetch({success: function(d, e) {
				var data = _.sortBy(e.data, function(o) {
					return -o.start_ts;
				})
				_.each(data, function(o){ 
					app.tasks.add(o);
				});
			}, reset: true});
		},

		addOneJob: function (job) {
			var view = new app.JobView({ model: job });
			this.$list.append(view.render().el);
		},

		addOneTask: function (task) {
			var view = new app.TaskView({ model: task });
			this.$list.append(view.render().el);
		},
	});
})(jQuery);