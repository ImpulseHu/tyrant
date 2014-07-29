$.fn.serializeObject = function() {
  var o = {};
  var a = this.serializeArray();
  $.each(a, function() {
    if (o[this.name] !== undefined) {
      if (!o[this.name].push) {
        o[this.name] = [o[this.name]];
      }
      o[this.name].push(this.value || '');
    } else {
      o[this.name] = this.value || '';
    }
  });
  return o;
};

var setFormValue = function(form_sel, obj) {
  for(var k in obj) {
    $(form_sel + ' input[name="' + k + '"]').val(obj[k]);
  }
}

$(document).ready(function(){

  $('#job-form').submit(function(e) {
    e.preventDefault();
    var action = $('#job-modal').data('action');
    var url = '/job';
    var type = 'post';

    if (action == 'edit') {
      url = '/job/' + $('#job-modal').data('id');
      type = 'put';
    }

    $.ajax({
      url : url,
      type: type,
      contentType: 'application/json',
      processData: false,
      data : JSON.stringify($(this).serializeObject()),
      success: function(e) {
        e = JSON.parse(e)
        if (e.ret == 0) {
          location.reload();
        }
      },
      error: function(xhr, text, err) {
        alert(xhr.responseText);
      }
    });
  });

  $('.new-btn').click(function() {
    $('#job-modal').modal('show');
    $('#job-modal').data('action', 'new');
    $('#job-modal input').each(function(idx, o) {
      if ($(o).attr('type') == 'text') {
        $(o).val('');
      }
    });
  })

  $('.edit-btn').click(function() {
    var job_id = $(this).data('id');
    $('#job-modal').data('action', 'edit');
    $('#job-modal').data('id', job_id);
    $.get('/job/' + job_id, {}, function(e) {
      e = JSON.parse(e);
      setFormValue('#job-form', e.data);
      $('#job-modal').modal('show');
    });
  });

  $('.remove-btn').click(function() {
    var job_id = $(this).data('id');
    $.ajax({
      url : '/job/' + job_id,
      type: 'DELETE',
      success: function(e) {
        e = JSON.parse(e)
        if (e.ret == 0) {
          $('#job-' + job_id).remove();
        }
      },
      error: function(xhr, text, err) {
        alert(xhr.responseText);
      }
    });
  });

  $('.run-btn').click(function() {
    var job_id = $(this).data('id');
    $.ajax({
      url : '/job/run/' + job_id,
      type: 'POST',
      success: function(e) {
        e = JSON.parse(e)
        if (e.ret == 0) {
          alert(e.data)
        }
      },
      error: function(xhr, text, err) {
        alert(xhr.responseText);
      }
    });
  });


})
