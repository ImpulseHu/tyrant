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

function TimestampToDate(ts) {
  // create a new javascript Date object based on the timestamp
  // multiplied by 1000 so that the argument is in milliseconds, not seconds
  var date = new Date(ts * 1000);
  // hours part from the timestamp
  var year =  date.getFullYear();
  var mon = date.getMonth();
  var day = date.getDay();
  var hours = date.getHours();
  // minutes part from the timestamp
  var minutes = date.getMinutes();
  // seconds part from the timestamp
  var seconds = date.getSeconds();
  // will display time in 10:30:23 format
  return year + '-' + mon + '-' + day + ' ' + hours + ':' + minutes + ':' + seconds;
}

