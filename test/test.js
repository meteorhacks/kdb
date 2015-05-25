var batchSize = parseInt(process.env.BATCH_SIZE)

meteorDown.init(function (Meteor) {
  function call () {
    var batch = newBatch(batchSize);
    Meteor.call('write', JSON.stringify(batch), function (err, res) {
      if(err) {
        console.error(err);
      }

      call();
    });
  }

  call();
});

meteorDown.run({
  concurrency: parseInt(process.env.CONCURRENCY),
  url: process.env.TARGETS
});

function randInt (n) {
  return Math.floor(n * Math.random());
}

function newBatch (n) {
  var batch = [];
  for(var i=0; i<n; ++i) {
    batch[i] = newPoint();
  }

  return batch;
}

function newPoint () {
  return {
    type: "type-"+randInt(5),
    value: randInt(10),
    samples: randInt(5),
    time: randInt(1440),
  };
}
