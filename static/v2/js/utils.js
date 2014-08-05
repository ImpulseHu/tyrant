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

function getUsername() {
    var match = document.cookie.match(new RegExp("username=([^;]+)"));
    if (match) {
        return match[1];
    }
}