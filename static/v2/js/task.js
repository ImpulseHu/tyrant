$(document).ready(function(){

  $('.kill-btn').click(function() {
    var task_id = $(this).data('id');
    $.ajax({
      url : '/task/kill/' + task_id,
      type: 'GET',
      success: function(e) {
        alert(e);
      },
      error: function(xhr, text, err) {
        alert(xhr.responseText);
      }
    });
  });

});
