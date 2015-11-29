var heatmap;
var map;

function initMap() {

  var mapOptions = {
    zoom: 2,
    center: {
      lat: 17.7850,
      lng: -12.4183
    },
    mapTypeId: google.maps.MapTypeId.TERRAIN,
    mapTypeControl: true,
    mapTypeControlOptions: {
      style: google.maps.MapTypeControlStyle.HORIZONTAL_BAR,
      position: google.maps.ControlPosition.LEFT_BOTTOM
    }
  };
  
  map = new google.maps.Map(document.getElementById("map"), mapOptions);

  var liveTweets = new google.maps.MVCArray();
  
  heatmap = new google.maps.visualization.HeatmapLayer({
    data: liveTweets,
    radius: 25
  });
  
  heatmap.setMap(map);

  if (io !== undefined) {
    // connect to socket.io
    var socket = io.connect();

    // listening for event 'tweets' from
    socket.on('tweets', function (data) {
      console.log(data)
      // push new tweets into the map
      var tweetLocation = new google.maps.LatLng(parseFloat(data.longitude), parseFloat(data.latitude));
      liveTweets.push(tweetLocation);

      // mark the new added position in the map
      var image = "css/twitter.png";
      var marker = new google.maps.Marker({
        position: tweetLocation,
        map: map,
        icon: image
      });
      setTimeout(function () {
        marker.setMap(null);
      }, 600);

    });
  }
}

function toggleHeatmap() {
  heatmap.setMap(heatmap.getMap() ? null : map);
}

function changeGradient() {
  var gradient = [
    'rgba(0, 255, 255, 0)',
    'rgba(0, 255, 255, 1)',
    'rgba(0, 191, 255, 1)',
    'rgba(0, 127, 255, 1)',
    'rgba(0, 63, 255, 1)',
    'rgba(0, 0, 255, 1)',
    'rgba(0, 0, 223, 1)',
    'rgba(0, 0, 191, 1)',
    'rgba(0, 0, 159, 1)',
    'rgba(0, 0, 127, 1)',
    'rgba(63, 0, 91, 1)',
    'rgba(127, 0, 63, 1)',
    'rgba(191, 0, 31, 1)',
    'rgba(255, 0, 0, 1)'
  ]
  heatmap.set('gradient', heatmap.get('gradient') ? null : gradient);
}

function changeRadius() {
  heatmap.set('radius', heatmap.get('radius') ? null : 20);
}

function changeOpacity() {
  heatmap.set('opacity', heatmap.get('opacity') ? null : 0.2);
}