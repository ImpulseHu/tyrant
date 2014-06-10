var app = app || {};
(function ($) {
	'use strict';

	app.JobNewView = Backbone.View.extend({

		el : '#new-job-view',

		events: {
			'click .save-btn': 'onSaveClick',
		},

		initialize: function () {
			this.$name = $('#name');
			this.$executor = $('#executor');
			this.$executor_flag = $('#executor-flag');
			this.$owner = $('#owner');
		},

		onSaveClick : function () {
			var job = new app.Job();
			job.save({
				name: this.$name.val(),
				executor: this.$executor.val(),
				executor_flag : this.$executor_flag.val(),
				owner : this.$owner.val()
			}, {
				success : function(j, e) {
					j.set('id', e.data.id);
					app.jobs.add(j);
				},
				error: function(j, e) {
					alert(e.responseJSON.msg);
				}
			});
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
	    	    executor_flag : $(".executor-flag-input").val(),
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
		},

		initialize: function () {
			this.listenTo(this.model, 'change', this.render);
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
			this.$list = $('#job-list');
			this.listenTo(app.jobs, 'add', this.addOne);
			app.jobs.fetch({success: function(d, e) {
				_.each(e.data, function(o){ 
					app.jobs.add(o); 
				});
			}, reset: true});
		},

		render: function () {
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