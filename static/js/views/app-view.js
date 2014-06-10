var app = app || {};
(function ($) {
	'use strict';

	app.JobNewView = Backbone.View.extend({
		template : _.template($("#job-new-template").html()),

		events: {
			'click .save-btn': 'onSaveClick',
		},

		initialize: function () {
		},

		onSaveClick : function () {
			var job = new app.Job();
			job.save({
				name: $('#name').val(),
				executor: $('#executor').val(),
				executor_flags : $('#executor-flag').val(),
				uris : $('#uris').val(),
				owner : $('#owner').val()
			}, {
				success : function(j, e) {
					j.set('id', e.data.id);
					app.jobs.add(j);
				},
				error: function(j, e) {
					alert(e.responseJSON.msg);
				}
			});
		},

		render: function() {
          this.$el.html(this.template());
          return this;
        }
	});

	app.ModalContent = Backbone.View.extend({

		template : _.template($("#edit-modal-template").html()),

		initialize: function () {
	        this.bind("ok", this.onOkClicked);
	    },

	    onOkClicked: function (modal) {
	    	this.model.save(
	    	{
	    		name : $(".name-input").val(),
	    		executor : $(".executor-input").val(),
	    	    executor_flags : $(".executor-flag-input").val(),
				uris : $('.uris-input').val(),
	    	    owner : $(".owner-input").val()
	    	});
	        //modal.preventClose();
	    },

        render: function() {
          this.$el.html(this.template(this.model.toJSON()));
          return this;
        }
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
			$.post('/job/run/' + this.model.get('id'), function(e) {
				alert(e);
			})
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
			this.listenTo(app.jobs, 'add', this.addOne);
		},

		render: function (page) {
			if (page == 'job') {
				this.jobPage();
			}
			else if (page == 'status') {
				this.statusPage();
			}
		},

		jobPage : function () {
			var list_tmpl = _.template($("#job-view-template").html());
			var newJobView = new app.JobNewView();
			this.$el.html(list_tmpl());
			$('#new-job-form').html(newJobView.render().el);

 			this.$list = $('#job-list');

 			app.jobs.fetch({success: function(d, e) {
				_.each(e.data, function(o){ 
					app.jobs.add(o); 
				});
			}, reset: true});
		},

		statusPage : function() {
			var tmpl = _.template($("#status-view-template").html())
			this.$el.html(tmpl());
		},

		addOne: function (job) {
			var view = new app.JobView({ model: job });
			this.$list.append(view.render().el);
		},

		addAll: function () {
			this.$list.html('');
			app.jobs.each(this.addOne, this);
		}
	});
})(jQuery);