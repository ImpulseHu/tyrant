$(document).ready(function(){

  var changeURLPar = function(destiny, par, par_value) {
    var pattern = par+'=([^&]*)';
    var replaceText = par+'='+par_value;
    if (destiny.match(pattern)) {
      var tmp = '/\\'+par+'=[^&]*/';
      tmp = destiny.replace(eval(tmp), replaceText);
      return (tmp);
    }
    else {
      if (destiny.match('[\?]')) {
        return destiny+'&'+ replaceText;
      }
      else {
        return destiny+'?'+replaceText;
      }
    }
    return destiny+'\n'+par+'\n'+par_value;
  }

  $('.go-page').click(function(e) {
    var page = $(this).data('page');
    var limit = $(this).data('limit');

    var new_url = window.location.href;
    new_url = changeURLPar(new_url, "page", page);
    new_url = changeURLPar(new_url, "limit", limit);
    window.location.href = new_url;
  })

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
