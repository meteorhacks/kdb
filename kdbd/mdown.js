meteorDown.init(function (Meteor) {
  function call () {
    var batch = newBatch(1000);
    Meteor.call('put', JSON.stringify(batch), function (err, res) {
      if(err) {
        console.error(err);
      }

      call();
    });
  }

  call();
});

meteorDown.run({
  concurrency: 5,
  url: 'http://localhost:3000'
});

function rand (n) {
  return Math.floor(n * Math.random());
}

function newBatch (n) {
  var time = getTimeNs();
  var batch = [];

  for(var i=0; i<n; ++i) {
    batch[i] = newPoint(time);
  }

  return {points: batch};
}

function newPoint (time) {
  var buffer = new Buffer(16);
  buffer.writeDoubleLE(20 + rand(80), 0);
  buffer.writeDoubleLE(10 + rand(10), 8);

  var appId = rand(1000);
  var part = appId % 4;

  return {
    timestamp: time,
    partition: part,
    indexValues: ["a"+appId, "t"+rand(20), "h"+rand(5), "d"+rand(10)],
    payload: buffer.toString('base64'),
  }
}

function getTimeNs () {
  var now = Date.now()
  now -= now % 60000;
  return now * 1000000;
}
