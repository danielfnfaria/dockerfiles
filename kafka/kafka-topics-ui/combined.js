'use strict';

var angularAPP = angular.module('angularAPP', [
  'ngRoute',
  'ngMaterial',
  'ngAnimate',
  'ngCookies',
  'md.data.table',
  'ngAria',
  'ui.ace',
  'angularUtils.directives.dirPagination',
  'angular-json-tree',
  'env',
  'HttpFactory',
  'topicsList',
  'totalBrokers',
  'totalTopics',
  'flatView',
  'treeView',
  'ngHandsontable',
  'rawView',
  'base64'
]);

//angularAPP.controller('HeaderCtrl', function (env, $rootScope, $scope, $log, $location, $route) { });

angularAPP.run(
    function loadRoute( env, $routeParams, $rootScope, $location, $http ) {
        $rootScope.$on('$routeChangeSuccess', function() {
            //When the app starts set the envs
            if(!env.isMissingEnvJS()) {
                 env.setSelectedCluster($routeParams.cluster);
                 $rootScope.clusters = env.getAllClusters();
                 $rootScope.cluster = env.getSelectedCluster();
            } else {
                 $rootScope.missingEnvJS = env.isMissingEnvJS();
            }
       });

       $rootScope.selectCluster = function(cluster) {
           $rootScope.connectionFailure = false;
           $location.path("/cluster/"+cluster)
           $rootScope.cluster = cluster;
       }

       //TODO Where to check connectivity and make it public for all components ?
    }
)

angularAPP.run(['$route', '$rootScope', '$location', function ($route, $rootScope, $location) {
    var original = $location.path;
    $location.path = function (path, reload) {
        if (reload === false) {
            var lastRoute = $route.current;
            var un = $rootScope.$on('$locationChangeSuccess', function () {
                $route.current = lastRoute;
                un();
            });
        }
        return original.apply($location, [path]);
    };
}]);

angularAPP.config(function($logProvider){
  $logProvider.debugEnabled(true); //todo get from env
});

angularAPP.config(function ($routeProvider, $locationProvider) {
$locationProvider.html5Mode();
  $locationProvider.hashPrefix('');
  $routeProvider
    .when('/', {
      templateUrl: 'src/kafka-topics/home/home.html',
      controller: 'HomeCtrl'
    })
    .when('/healthcheck', {
      templateUrl: 'src/kafka-topics/healthcheck/healthcheck.html',
      controller: 'HealthcheckCtrl'
    })
    .when('/cluster/:cluster', {
      templateUrl: 'src/kafka-topics/home/home.html',
       controller: 'HomeCtrl'
    })
    .when('/cluster/:cluster/create-topic', {
      templateUrl: 'src/kafka-topics/new/new-topic.html',
      controller: 'HeaderCtrl'
    })
    .when('/cluster/:cluster/topic/:topicCategoryUrl/:topicName/', {
        templateUrl: 'src/kafka-topics/view/view.html',
        controller: 'ViewTopicCtrl'
      })
    .when('/cluster/:cluster/topic/:topicCategoryUrl/:topicName/:menuItem', {
      templateUrl: 'src/kafka-topics/view/view.html',
      controller: 'ViewTopicCtrl'
    })
    .when('/cluster/:cluster/topic/:topicCategoryUrl/:topicName/:menuItem/:selectedTabIndex', {
      templateUrl: 'src/kafka-topics/view/view.html',
      controller: 'ViewTopicCtrl'
    }).otherwise({
    redirectTo: '/'
  });
  // $locationProvider.html5Mode(true);
});

angularAPP.run(['$route', '$rootScope', '$location', function ($route, $rootScope, $location) {
    var original = $location.path;
    $location.path = function (path, reload) {
        if (reload === false) {
            var lastRoute = $route.current;
            var un = $rootScope.$on('$locationChangeSuccess', function () {
                $route.current = lastRoute;
                un();
            });
        }
        return original.apply($location, [path]);
    };
}])

// ng-show="x | isEmpty"
angularAPP.filter('isEmpty', function () {
  var bar;
  return function (obj) {
    for (bar in obj) {
      if (obj.hasOwnProperty(bar)) {
        return false;
      }
    }
    return true;
  };
});

angularAPP.filter("sanitize", ['$sce', function ($sce) {
  return function (htmlCode) {
    return $sce.trustAsHtml(htmlCode);
  }
}]);

angularAPP.config(function ($mdThemingProvider) {
  $mdThemingProvider.theme('default')
    .primaryPalette('blue-grey')
    .accentPalette('blue')
    .warnPalette('grey');
});


angularAPP.filter('humanize', function(){
    return function humanize(number) {
        if(number < 1000) {
            return number;
        }
        var si = ['K', 'M', 'G', 'T', 'P', 'H'];
        var exp = Math.floor(Math.log(number) / Math.log(1000));
        var result = number / Math.pow(1000, exp);
        result = (result % 1 > (1 / Math.pow(1000, exp - 1))) ? result.toFixed(2) : result.toFixed(0);
        return result + si[exp - 1];
    };
});

var KAFKA_DEFAULTS =
  [{
    "property": "cleanup.policy",
    "default": "delete",
    "serverDefaultProperties": "log.cleanup.policy",
    "description": "A string that is either \"delete\" or \"compact\". This string designates the retention policy to use on old log segments. The default policy (\"delete\") will discard old segments when their retention time or size limit has been reached. The \"compact\" setting will enable log compaction on the topic."
  }, {
    "property": "delete.retention.ms",
    "default": "86400000",
    "serverDefaultProperties": "log.cleaner.delete.retention.ms",
    "description": "The amount of time to retain delete tombstone markers for log compacted topics. This setting also gives a bound on the time in which a consumer must complete a read if they begin from offset 0 to ensure that they get a valid snapshot of the final stage (otherwise delete tombstones may be collected before they complete their scan). Default is 24 hours"
  }, {
    "property": "flush.messages",
    "default": "None",
    "serverDefaultProperties": "log.flush.interval.messages",
    "description": "This setting allows specifying an interval at which we will force an fsync of data written to the log. For example if this was set to 1 we would fsync after every message; if it were 5 we would fsync after every five messages. In general we recommend you not set this and use replication for durability and allow the operating system's background flush capabilities as it is more efficient. This setting can be overridden on a per-topic basis (see the per-topic configuration section)."
  }, {
    "property": "flush.ms",
    "default": "None",
    "serverDefaultProperties": "log.flush.interval.ms",
    "description": "This setting allows specifying a time interval at which we will force an fsync of data written to the log. For example if this was set to 1000 we would fsync after 1000 ms had passed. In general we recommend you not set this and use replication for durability and allow the operating system's background flush capabilities as it is more efficient."
  }, {
    "property": "index.interval.bytes",
    "default": "4096",
    "serverDefaultProperties": "log.index.interval.bytes",
    "description": "This setting controls how frequently Kafka adds an index entry to it's offset index. The default setting ensures that we index a message roughly every 4096 bytes. More indexing allows reads to jump closer to the exact position in the log but makes the index larger. You probably don't need to change this."
  }, {
    "property": "max.message.bytes",
    "default": "1000000",
    "serverDefaultProperties": "message.max.bytes",
    "description": "This is largest message size Kafka will allow to be appended to this topic. Note that if you increase this size you must also increase your consumer's fetch size so they can fetch messages this large."
  }, {
    "property": "min.cleanable.dirty.ratio",
    "default": "0.5",
    "serverDefaultProperties": "log.cleaner.min.cleanable.ratio",
    "description": "This configuration controls how frequently the log compactor will attempt to clean the log (assuming log compaction is enabled). By default we will avoid cleaning a log where more than 50% of the log has been compacted. This ratio bounds the maximum space wasted in the log by duplicates (at 50% at most 50% of the log could be duplicates). A higher ratio will mean fewer, more efficient cleanings but will mean more wasted space in the log."
  }, {
    "property": "min.insync.replicas",
    "default": "1",
    "serverDefaultProperties": "min.insync.replicas",
    "description": "When a producer sets acks to \"all\", min.insync.replicas specifies the minimum number of replicas that must acknowledge a write for the write to be considered successful. If this minimum cannot be met, then the producer will raise an exception (either NotEnoughReplicas or NotEnoughReplicasAfterAppend). When used together, min.insync.replicas and acks allow you to enforce greater durability guarantees. A typical scenario would be to create a topic with a replication factor of 3, set min.insync.replicas to 2, and produce with acks of \"all\". This will ensure that the producer raises an exception if a majority of replicas do not receive a write."
  }, {
    "property": "retention.bytes",
    "default": "None",
    "serverDefaultProperties": "log.retention.bytes",
    "description": "This configuration controls the maximum size a log can grow to before we will discard old log segments to free up space if we are using the \"delete\" retention policy. By default there is no size limit only a time limit."
  }, {
    "property": "retention.ms",
    "default": "7 days",
    "serverDefaultProperties": "log.retention.minutes",
    "description": "This configuration controls the maximum time we will retain a log before we will discard old log segments to free up space if we are using the \"delete\" retention policy. This represents an SLA on how soon consumers must read their data."
  }, {
    "property": "segment.bytes",
    "default": "1 GB",
    "serverDefaultProperties": "log.segment.bytes",
    "description": "This configuration controls the segment file size for the log. Retention and cleaning is always done a file at a time so a larger segment size means fewer files but less granular control over retention."
  }, {
    "property": "segment.index.bytes",
    "default": "10 MB",
    "serverDefaultProperties": "log.index.size.max.bytes",
    "description": "This configuration controls the size of the index that maps offsets to file positions. We preallocate this index file and shrink it only after log rolls. You generally should not need to change this setting."
  }, {
    "property": "segment.ms",
    "default": "7 days",
    "serverDefaultProperties": "log.roll.hours",
    "description": "This configuration controls the period of time after which Kafka will force the log to roll even if the segment file isn't full to ensure that retention can delete or compact old data."
  }, {
    "property": "segment.jitter.ms",
    "default": "0",
    "serverDefaultProperties": "log.roll.jitter.{ms,hours}",
    "description": "The maximum jitter to subtract from logRollTimeMillis."
  }];
var KNOWN_TOPICS = {
  JSON_TOPICS: ["_schemas"],
  BINARY_TOPICS: ["connect-configs",
                  "connect-offsets",
                  "__consumer_offsets",
                  "_confluent-monitoring",
                  "_confluent-controlcenter",
                  "connect-statuses",
                  "__confluent.support.metr"],
  // If a topic starts with this particular prefix - it's a system topic
  CONTROL_TOPICS: ["_confluent-controlcenter", "_confluent-command", "_confluent-metrics", "connect-configs", "connect-offsets", "__confluent", "__consumer_offsets", "_confluent-monitoring", "connect-status", "_schemas"]
  };

//var TOPIC_CONFIG = {
//  KAFKA_TOPIC_DELETE_COMMAND : "kafka-topics --zookeeper zookeeper-host:2181/confluent --delete --topic",
//  // Pre-configure the Data Type on particular well-known topics
//  JSON_TOPICS: ["_schemas"],
//  BINARY_TOPICS: ["connect-configs", "connect-offsets", "__consumer_offsets", "_confluent-monitoring", "_confluent-controlcenter", "__confluent.support.metr"],
//  // If a topic starts with this particular prefix - it's a control topic
//  CONTROL_TOPICS: ["_confluent-controlcenter", "_confluent-command", "_confluent-metrics", "connect-configs", "connect-offsets", "__confluent", "__consumer_offsets", "_confluent-monitoring", "connect-status", "_schemas"]
//  };

angularAPP.factory('consumerFactory', function ($rootScope, $http, $log, $q, $filter, $cookies, env, HttpFactory) {


  var CONTENT_TYPE_JSON = 'application/vnd.kafka.v2+json';
  var CONSUMER_NAME_PREFIX = 'kafka-topics-ui-';
  var PRINT_DEBUG_CURLS = false;

  /**
   * Creates consumer + group with unique uuid and type in name.
   **/
  function createConsumer(format, topicName, uuid) {
    $log.debug(topicName, "CREATING CONSUMER: ", getConsumer(format, uuid), uuid);
    var url = env.KAFKA_REST().trim() + '/consumers/' + getConsumer(format, uuid).group;
    var data = '{"name": "' + getConsumer(format).instance + '", "format": "' + format + '", "auto.offset.reset": "earliest", "auto.commit.enable": "false"}';
    return HttpFactory.req('POST', url, data, CONTENT_TYPE_JSON, '', true, PRINT_DEBUG_CURLS);
  }

  /**
   * Waits for the pre-requisite requests to be done and then
   * starts polling records (/records).
   * When gets the records, deletes the consumer
   **/
  function getDataFromBeginning(consumer, format, topicName) {
    return $q.all([seekAll('beginning', consumer, topicName)]).then(function (res1) {
      $log.debug(topicName, '4) SEEK TO BEGGINING FOR ALL PARTITIONS DONE');
      $log.debug(topicName, "5) START POLLING WITH CONSUMER:", consumer);
    }).then(function (res2) {
      return getRecords(consumer, format).then(function (r) {
        if (r.data.length !== 0) saveTopicTypeToCookie(topicName, format);
        $log.debug(topicName, '6) DONE: GOT RECORDS', r.data.length, r);
        $log.debug(topicName, '7) SAVING TYPE TO COOKIE', format);
        deleteConsumer(consumer, topicName);
        return r;
      }, function (er) {
        $log.error("CANNOT GET RECORDS WITH FORMAT", format);
        deleteConsumer(consumer, topicName);
        return -1;
      });
    });
  }
  function getDataFromEnd(consumer, format, topicName) {
    return $q.all([seekAll('end', consumer, topicName)]).then(function (res1) {
      $log.debug(topicName, '4) SEEK TO BEGGINING FOR ALL PARTITIONS DONE');
      $log.debug(topicName, "5) START POLLING WITH CONSUMER:", consumer);
    }).then(function (res2) {
      return getRecords(consumer, format).then(function (r) {
        if (r.data.length !== 0) saveTopicTypeToCookie(topicName, format);
        $log.debug(topicName, '6) DONE: GOT RECORDS', r.data.length);
        $log.debug(topicName, '7) SAVING TYPE TO COOKIE', format);
        deleteConsumer(consumer, topicName);
        return r;
      }, function (er) {
        $log.error("CANNOT GET RECORDS WITH FORMAT", format);
        deleteConsumer(consumer, topicName);
        return -1;
      });
    });
  }

  function getDataForPartition(topicName, consumer, format, partition, offset, position) {
    return postConsumerAssignments(consumer, topicName, partition).then(function (responseAssign) {
      return postConsumerPositions(consumer, topicName, partition[0], offset, position).then(function (responseOffset) {
        $log.debug(topicName, '4) SEEK TO OFFSET FOR PARTITION DONE');
        $log.debug(topicName, "5) START POLLING WITH CONSUMER:", consumer);
        return getRecords(consumer, format).then(function (r) {
          $log.debug(topicName, '6) DONE: GOT RECORDS', r.data.length);
          $log.debug(topicName, '7) SAVING TYPE TO COOKIE', format);
          deleteConsumer(consumer, topicName);
          return r;
        }, function (er) {
          $log.error("CANNOT GET RECORDS WITH FORMAT", format);
          deleteConsumer(consumer, topicName);
          return -1;
        });
      });
    });
  }

  /**
   * Does all the required requests before polling
   * 1) Gets the paritions for topic (/partitions)
   * 2) Assigns ALL the partitions to consumer (/assignments)
   * 3) Moves all the partitions to beginning (/positions/beginning)
   * TODO pass the partitions because
   * TODO         a) we have them so no need for requests b)
   * TODO         a) will make it generic to be used for 1 partition as well
   * TODO                      seekForPartition(topicName, consumer, beginningOrEnd, partition, offset)
   **/
  function seekAll(beginningOrEnd, consumer, topicName) {
    $log.debug(topicName, "POLL STEPS START");
    return getPartitions(topicName).then(function (partitions) {
      $log.debug(topicName, '1) DONE: GOT ALL PARTITIONS', partitions);
      return postConsumerAssignments(consumer, topicName, partitions.data).then(function (r) {
        $log.debug(topicName, '3) DONE: ASSIGNED PARTITIONS TO CONSUMER');
        var url = env.KAFKA_REST().trim() + '/consumers/' + consumer.group + '/instances/' + consumer.instance + '/positions/' + beginningOrEnd;
        var data = preparePartitionData(topicName, partitions.data);
        return HttpFactory.req('POST', url, data, CONTENT_TYPE_JSON, '', false, PRINT_DEBUG_CURLS);
      })
    });
  }

  function getConsumerOffsets(consumer, topicName, partition) {
        var url = env.KAFKA_REST().trim() + '/consumers/' + consumer.group + '/instances/' + consumer.instance + '/offsets';
        var data = {"partitions": [{"topic": topicName,"partition": parseInt(partition[0].partition)}]}
        return HttpFactory.req('GET', url, data, '', CONTENT_TYPE_JSON, false,  true);
      }

  /* PRIMITIVE REQUESTS RETURN PROMISES */

  function postConsumerAssignments(consumer, topicName, partitions) {
//    return deleteConsumerSubscriptions(consumer).then(function(responseDelete){
    var data = preparePartitionData(topicName, partitions);
    $log.debug(topicName, "2) ACTUAL PARTITIONS TO ASSIGN", data);
    var url = env.KAFKA_REST().trim() + '/consumers/' + consumer.group + '/instances/' + consumer.instance + '/assignments';
    return HttpFactory.req('POST', url, data, CONTENT_TYPE_JSON, '', false, PRINT_DEBUG_CURLS);
//    })
  }

  function getConsumerAssignments(consumer) {
    var url_tmp = env.KAFKA_REST().trim() + '/consumers/' + consumer.group + '/instances/' + consumer.instance + '/assignments';
    return HttpFactory.req('GET', url_tmp, '', '', '', false, false).then(function (res) {
    })
  }

  function getPartitions(topicName) {
    var url = env.KAFKA_REST().trim() + '/topics/' + topicName + '/partitions';
    return HttpFactory.req('GET', url, '', '', 'application/vnd.kafka.v2+json, application/vnd.kafka+json, application/json', false, PRINT_DEBUG_CURLS);
  }

  function getRecords(consumer, format) {
    var url = env.KAFKA_REST().trim() + '/consumers/' + consumer.group + '/instances/' + consumer.instance + '/records?timeout=' + env.RECORD_POLL_TIMEOUT() + '&max_bytes=' + env.MAX_BYTES().trim();
    var ACCEPT_HEADER = 'application/vnd.kafka.' + format + '.v2+json';
    return HttpFactory.req('GET', url, '', CONTENT_TYPE_JSON, ACCEPT_HEADER, false, PRINT_DEBUG_CURLS);
  }

  function deleteConsumer(consumer, topicName) {
    HttpFactory.req('DELETE', env.KAFKA_REST().trim() + '/consumers/' + consumer.group + '/instances/' + consumer.instance, '', CONTENT_TYPE_JSON, '', false, false)
      .then(function (res) {
        $log.debug(topicName, "8) CONSUMER DELETED", consumer);
        $cookies.remove('uuid')
      })
  }

  function postConsumerPositions(consumer, topicName, partition, offset, position) {

    switch (position) {
      case 'beginning':
        var data = {'partitions': [{'topic': topicName, 'partition': partition.partition}]};
        $log.debug(topicName, "3) SEEK PARTITION TO BEGINNING", data);
        var url = env.KAFKA_REST().trim() + '/consumers/' + consumer.group + '/instances/' + consumer.instance + '/positions/beginning';
        return HttpFactory.req('POST', url, data, CONTENT_TYPE_JSON, '', true, PRINT_DEBUG_CURLS);
        break;
      case 'end':
        var data = {'partitions': [{'topic': topicName, 'partition': partition.partition}]};
        $log.debug(topicName, "3) SEEK PARTITION TO END", data);
        var url = env.KAFKA_REST().trim() + '/consumers/' + consumer.group + '/instances/' + consumer.instance + '/positions/end';
        return HttpFactory.req('POST', url, data, CONTENT_TYPE_JSON, '', true, PRINT_DEBUG_CURLS);
        break;
      case 'offset':
        var data = {'offsets': [{'topic': topicName, 'partition': partition.partition, 'offset': offset}]};
        $log.debug(topicName, "3) SEEK TO OFFSETS", data);
        var url = env.KAFKA_REST().trim() + '/consumers/' + consumer.group + '/instances/' + consumer.instance + '/positions';
        return HttpFactory.req('POST', url, data, CONTENT_TYPE_JSON, '', true, PRINT_DEBUG_CURLS);
        break;
      default:
        $log.debug("Not a valid position", position)
    }
  }

  //UTILITIES / STATICS

  function getConsumer(format, uuid) {
    return {group: 'kafka_topics_ui_' + format + '_' + uuid, instance: CONSUMER_NAME_PREFIX + format};
  }

  function preparePartitionData(topicName, partitions) {
    var data = {'partitions': []};
    angular.forEach(partitions, function (partition) {
      data.partitions.push({'topic': topicName, 'partition': partition.partition})
    });
    return data;
  }

  function consumerUUID() {
    var a = $filter('date')(Date.now(), "yyyy-MM-dd-hh-mm-ss-sss");
    //$cookies.put('uuid', $filter('date')(Date.now(), "yyyy-MM-dd-hh-mm-ss-sss")); //TODO milis, do we need the cookie ?
    return a;
  }

  function saveTopicTypeToCookie(topicName, format) {
    var expireDate = new Date();
    expireDate.setDate(expireDate.getDate() + 1);
    $cookies.put(topicName, format, {'expires': expireDate});
  }

  function hasCookieType(topicName) {
    var a = $cookies.getAll();
    return a[topicName] ? true : false;
  }

  function isKnownBinaryTopic(topicName) {
    var a = false;
    angular.forEach(KNOWN_TOPICS.BINARY_TOPICS, function (t) {  //todo filter
      if (t == topicName) a = true;
    });
    return a;
  }

  function isKnownJSONTopic(topicName) {
    var a = false;
    angular.forEach(KNOWN_TOPICS.JSON_TOPICS, function (t) {  //todo filter
      if (t == topicName) {
        a = true;
      }
    });
    return a;
  }

  /**
   * If topic is not defined, or hasn't been consumed before, then will try detection start with Avro
   **/
  function getConsumerType(topicName) {
    if (isKnownBinaryTopic(topicName)) {
      $log.debug(topicName, "DETECTING TYPE.. IT'S A KNOWN [ BINARY ] TOPIC [topics.config.js]");
      return 'binary';
    }
    if (isKnownJSONTopic(topicName)) {
      $log.debug(topicName, "DETECTING TYPE.. IT'S A KNOWN [ JSON ] TOPIC [topics.config.js]");
      return 'json';
    } else if (hasCookieType(topicName)) {
      var a = $cookies.getAll();
      $log.debug(topicName, "DETECTING TYPE.. HAVE CONSUMED THIS TOPIC BEFORE, IT'S IN COOKIE. TYPE IS [" + a[topicName] + "]");
      return a[topicName];
    } else {
      $log.debug(topicName, "DETECTING TYPE.. DON'T KNOW THE TYPE I WILL TRY WITH [ AVRO ] FIRST");
      return 'avro';
    }
  }

  function getConsumerTypeRetry(previousFormatTried, topicName) {
    switch (previousFormatTried) {
      case 'avro':
        $log.debug(topicName, "DETECTING TYPE.. FAILED WITH AVRO, WILL TRY [ JSON ]");
        return 'json';
        break;
      case 'json':
        $log.debug(topicName, "DETECTING TYPE.. FAILED WITH JSON, WILL TRY [ BINARY ]");
        return 'binary';
        break;
      default:
        $log.debug(topicName, "DETECTING TYPE.. FAILED WITH AVRO & JSON, WILL TRY [ BINARY ]");
        return 'binary';
    }
  }

  //PUBLIC METHODS // TODO cleanup

  return {
    createConsumer: function (format, topicName, uuid) {
      return createConsumer(format, topicName, uuid);
    },
    getConsumer: function (format, uuid) {
      return getConsumer(format, uuid);
    },
    getConsumerType: function (topicName) {
      return getConsumerType(topicName);
    },
    getConsumerTypeRetry: function (previousFormatTried, topicName) {
      return getConsumerTypeRetry(previousFormatTried, topicName);
    },
    getDataFromBeginning: function (consumer, format, topicName) {
      return getDataFromBeginning(consumer, format, topicName);
    },
    getDataFromEnd: function (consumer, format, topicName) {
      return getDataFromEnd(consumer, format, topicName);
    },
    seekAll: function (beginningOrEnd, consumer, topicName, partition) {
      return seekAll(beginningOrEnd, consumer, topicName, partition);
    },
    postConsumerPositions: function (consumer, topicName, partition, offset) {
      return postConsumerPositions(consumer, topicName, partition, offset);
    },
    postConsumerAssignments: function (consumer, topicName, partitions) {
      return postConsumerAssignments(consumer, topicName, partitions);
    },
    getRecords: function (consumer, format) {
      return getRecords(consumer, format);
    },
    getConsumerOffsets: function (consumer, topicName, partition) {
      return getConsumerOffsets(consumer, topicName, partition);
    },
    getConsumerAssignments: function (consumer) {
      return getConsumerAssignments(consumer);
    },
    getPartitions: function (topicName) {
      return getPartitions(topicName);
    },
    genUUID: function () {
      return consumerUUID();
    },
    getDataForPartition: function (topicName, consumer, format, partition, offset, position) {
      return getDataForPartition(topicName, consumer, format, partition, offset, position);
    }
  }
});

angular.
    module("env", ['HttpFactory']).
    factory('env', function ($http) {

          var allClusters = (typeof clusters !== "undefined") ? angular.copy(clusters) : [];
          var selectedCluster = null;
          var missingEnvJS = false;
          var connectivityError = false;

          setCluster();

          return {
            setSelectedCluster : function(clusterName) { setCluster(clusterName) },
            getSelectedCluster : function() { return selectedCluster; },
            getAllClusters : function() { return allClusters },
            isMissingEnvJS : function() { return missingEnvJS},
            KAFKA_REST : function () { return selectedCluster.KAFKA_REST.trim(); },
            RECORD_POLL_TIMEOUT: function () { return selectedCluster.RECORD_POLL_TIMEOUT.trim(); },
            MAX_BYTES : function () { return selectedCluster.MAX_BYTES.trim(); },
            DEBUG_LOGS_ENABLED : function () { return selectedCluster.DEBUG_LOGS_ENABLED.trim(); }
          };

          function setCluster(clusterName) {
            if(allClusters.length === 0) {
                setMissingEnvJS(true);
                console.log("File [env.js] does not exist")
             }
             if(angular.isUndefined(clusterName)) {
                  selectedCluster = allClusters[0];
             } else {
                  var filteredArray = allClusters.filter(function(el) {return el.NAME == clusterName});
                  selectedCluster = filteredArray.length == 1 ?  filteredArray[0]  : allClusters[0];
             }
          }

          function setMissingEnvJS(isMissing) {
            missingEnvJS = isMissing;
          }

          //TODO
//          function tryConnectivity(cluster) {
//            if(!missingEnvJS) {
//                console.log("RRR3", cluster.KAFKA_REST)
//                $http.get(cluster.KAFKA_REST).then(function(response) {
//                    console.log("RRR4", cluster.KAFKA_REST, response)
//                })
//            }
//          }

    });
angular.
    module("HttpFactory", []).
    factory('HttpFactory', function ($http, $log, $q) {

    function printDebugCurl(method, url, data, contentType){
          var curlCreateConsumer = 'curl -X '+ method +' -H "Content-Type: ' + contentType + '" ' + "--data '" + JSON.stringify(data) + "' " + url;
          $log.debug("HttpFactory:  " + curlCreateConsumer);
      }

    return {
        req: function(method, url, data, contentType, acceptType, resolveError, withDebug) {
             var deferred = $q.defer();
             var request = {
                   method: method,
                   url: url,
                   data: data,
                   dataType: 'json',
                   headers: {
                            'Content-Type': contentType,
                            'Accept': acceptType //'application/json'
                            }
                 };

             if(withDebug) printDebugCurl(method, url, data, contentType);

             $http(request)
             .then(function (response){
                  deferred.resolve(response)
                },function (responseError){
                    var msg = "Failed at method [" + method + "] [" + url + "] with error: \n" + JSON.stringify(responseError);
                    $log.error("HTTP ERROR: ",msg, '\nDATA SENT:', data);
                    if(resolveError && responseError.status === 409) deferred.resolve(responseError); //resolve conflicts to handle
                    else deferred.reject(msg);
                });

             return deferred.promise;
        }
    }

});
/**
 * dirPagination - AngularJS module for paginating (almost) anything.
 *
 *
 * Credits
 * =======
 *
 * Daniel Tabuenca: https://groups.google.com/d/msg/angular/an9QpzqIYiM/r8v-3W1X5vcJ
 * for the idea on how to dynamically invoke the ng-repeat directive.
 *
 * I borrowed a couple of lines and a few attribute names from the AngularUI Bootstrap project:
 * https://github.com/angular-ui/bootstrap/blob/master/src/pagination/pagination.js
 *
 * Copyright 2014 Michael Bromley <michael@michaelbromley.co.uk>
 *
 * @antonios 18-Aug-16 enhanced this with a material-design pagination template
 */

(function() {

    /**
     * Config
     */
    var moduleName = 'angularUtils.directives.dirPagination';
    var DEFAULT_ID = '__default';

    /**
     * Module
     */
    angular.module(moduleName, [])
        .directive('dirPaginate', ['$compile', '$parse', 'paginationService', dirPaginateDirective])
        .directive('dirPaginateNoCompile', noCompileDirective)
        .directive('dirPaginationControls', ['paginationService', 'paginationTemplate', dirPaginationControlsDirective])
        .filter('itemsPerPage', ['paginationService', itemsPerPageFilter])
        .service('paginationService', paginationService)
        .provider('paginationTemplate', paginationTemplateProvider)
        .run(['$templateCache',dirPaginationControlsTemplateInstaller]);

    function dirPaginateDirective($compile, $parse, paginationService) {

        return  {
            terminal: true,
            multiElement: true,
            priority: 100,
            compile: dirPaginationCompileFn
        };

        function dirPaginationCompileFn(tElement, tAttrs){

            var expression = tAttrs.dirPaginate;
            // regex taken directly from https://github.com/angular/angular.js/blob/v1.4.x/src/ng/directive/ngRepeat.js#L339
            var match = expression.match(/^\s*([\s\S]+?)\s+in\s+([\s\S]+?)(?:\s+as\s+([\s\S]+?))?(?:\s+track\s+by\s+([\s\S]+?))?\s*$/);

            var filterPattern = /\|\s*itemsPerPage\s*:\s*(.*\(\s*\w*\)|([^\)]*?(?=\s+as\s+))|[^\)]*)/;
            if (match[2].match(filterPattern) === null) {
                throw 'pagination directive: the \'itemsPerPage\' filter must be set.';
            }
            var itemsPerPageFilterRemoved = match[2].replace(filterPattern, '');
            var collectionGetter = $parse(itemsPerPageFilterRemoved);

            addNoCompileAttributes(tElement);

            // If any value is specified for paginationId, we register the un-evaluated expression at this stage for the benefit of any
            // dir-pagination-controls directives that may be looking for this ID.
            var rawId = tAttrs.paginationId || DEFAULT_ID;
            paginationService.registerInstance(rawId);

            return function dirPaginationLinkFn(scope, element, attrs){

                // Now that we have access to the `scope` we can interpolate any expression given in the paginationId attribute and
                // potentially register a new ID if it evaluates to a different value than the rawId.
                var paginationId = $parse(attrs.paginationId)(scope) || attrs.paginationId || DEFAULT_ID;

                // (TODO: this seems sound, but I'm reverting as many bug reports followed it's introduction in 0.11.0.
                // Needs more investigation.)
                // In case rawId != paginationId we deregister using rawId for the sake of general cleanliness
                // before registering using paginationId
                // paginationService.deregisterInstance(rawId);
                paginationService.registerInstance(paginationId);

                var repeatExpression = getRepeatExpression(expression, paginationId);
                addNgRepeatToElement(element, attrs, repeatExpression);

                removeTemporaryAttributes(element);
                var compiled =  $compile(element);

                var currentPageGetter = makeCurrentPageGetterFn(scope, attrs, paginationId);
                paginationService.setCurrentPageParser(paginationId, currentPageGetter, scope);

                if (typeof attrs.totalItems !== 'undefined') {
                    paginationService.setAsyncModeTrue(paginationId);
                    scope.$watch(function() {
                        return $parse(attrs.totalItems)(scope);
                    }, function (result) {
                        if (0 <= result) {
                            paginationService.setCollectionLength(paginationId, result);
                        }
                    });
                } else {
                    paginationService.setAsyncModeFalse(paginationId);
                    scope.$watchCollection(function() {
                        return collectionGetter(scope);
                    }, function(collection) {
                        if (collection) {
                            var collectionLength = (collection instanceof Array) ? collection.length : Object.keys(collection).length;
                            paginationService.setCollectionLength(paginationId, collectionLength);
                        }
                    });
                }

                // Delegate to the link function returned by the new compilation of the ng-repeat
                compiled(scope);

                // (TODO: Reverting this due to many bug reports in v 0.11.0. Needs investigation as the
                // principle is sound)
                // When the scope is destroyed, we make sure to remove the reference to it in paginationService
                // so that it can be properly garbage collected
                // scope.$on('$destroy', function destroyDirPagination() {
                //     paginationService.deregisterInstance(paginationId);
                // });
            };
        }

        /**
         * If a pagination id has been specified, we need to check that it is present as the second argument passed to
         * the itemsPerPage filter. If it is not there, we add it and return the modified expression.
         *
         * @param expression
         * @param paginationId
         * @returns {*}
         */
        function getRepeatExpression(expression, paginationId) {
            var repeatExpression,
                idDefinedInFilter = !!expression.match(/(\|\s*itemsPerPage\s*:[^|]*:[^|]*)/);

            if (paginationId !== DEFAULT_ID && !idDefinedInFilter) {
                repeatExpression = expression.replace(/(\|\s*itemsPerPage\s*:\s*[^|\s]*)/, "$1 : '" + paginationId + "'");
            } else {
                repeatExpression = expression;
            }

            return repeatExpression;
        }

        /**
         * Adds the ng-repeat directive to the element. In the case of multi-element (-start, -end) it adds the
         * appropriate multi-element ng-repeat to the first and last element in the range.
         * @param element
         * @param attrs
         * @param repeatExpression
         */
        function addNgRepeatToElement(element, attrs, repeatExpression) {
            if (element[0].hasAttribute('dir-paginate-start') || element[0].hasAttribute('data-dir-paginate-start')) {
                // using multiElement mode (dir-paginate-start, dir-paginate-end)
                attrs.$set('ngRepeatStart', repeatExpression);
                element.eq(element.length - 1).attr('ng-repeat-end', true);
            } else {
                attrs.$set('ngRepeat', repeatExpression);
            }
        }

        /**
         * Adds the dir-paginate-no-compile directive to each element in the tElement range.
         * @param tElement
         */
        function addNoCompileAttributes(tElement) {
            angular.forEach(tElement, function(el) {
                if (el.nodeType === 1) {
                    angular.element(el).attr('dir-paginate-no-compile', true);
                }
            });
        }

        /**
         * Removes the variations on dir-paginate (data-, -start, -end) and the dir-paginate-no-compile directives.
         * @param element
         */
        function removeTemporaryAttributes(element) {
            angular.forEach(element, function(el) {
                if (el.nodeType === 1) {
                    angular.element(el).removeAttr('dir-paginate-no-compile');
                }
            });
            element.eq(0).removeAttr('dir-paginate-start').removeAttr('dir-paginate').removeAttr('data-dir-paginate-start').removeAttr('data-dir-paginate');
            element.eq(element.length - 1).removeAttr('dir-paginate-end').removeAttr('data-dir-paginate-end');
        }

        /**
         * Creates a getter function for the current-page attribute, using the expression provided or a default value if
         * no current-page expression was specified.
         *
         * @param scope
         * @param attrs
         * @param paginationId
         * @returns {*}
         */
        function makeCurrentPageGetterFn(scope, attrs, paginationId) {
            var currentPageGetter;
            if (attrs.currentPage) {
                currentPageGetter = $parse(attrs.currentPage);
            } else {
                // If the current-page attribute was not set, we'll make our own.
                // Replace any non-alphanumeric characters which might confuse
                // the $parse service and give unexpected results.
                // See https://github.com/michaelbromley/angularUtils/issues/233
                var defaultCurrentPage = '_'+(paginationId + '__currentPage').replace(/\W/g, '_');
                scope[defaultCurrentPage] = 1;
                currentPageGetter = $parse(defaultCurrentPage);
            }
            return currentPageGetter;
        }
    }

    /**
     * This is a helper directive that allows correct compilation when in multi-element mode (ie dir-paginate-start, dir-paginate-end).
     * It is dynamically added to all elements in the dir-paginate compile function, and it prevents further compilation of
     * any inner directives. It is then removed in the link function, and all inner directives are then manually compiled.
     */
    function noCompileDirective() {
        return {
            priority: 5000,
            terminal: true
        };
    }

    function dirPaginationControlsTemplateInstaller($templateCache) {
      var strVar="";
      strVar += "<section  layout=\"row\" layout-align=\"center\" ng-if=\"1 < pages.length || !autoHide\" class=\"pagination\"> <md-button aria-label=\"Previous page\" ng-if=\"boundaryLinks\" ng-disabled=\"pagination.current===1\" ng-click=\"setCurrent(1)\"> <ng-md-icon icon=\"first_page\"><\/ng-md-icon> <\/md-button> <md-button aria-label=\"First page\" ng-if=\"directionLinks\" ng-disabled=\"pagination.current===1\" ng-click=\"setCurrent(pagination.current - 1)\"> <ng-md-icon class=\"fa fa-chevron-left\"><\/ng-md-icon> <\/md-button> <md-button ng-repeat=\"pageNumber in pages track by tracker(pageNumber, $index)\" ng-class=\"{'md-primary' : pagination.current==pageNumber}\" ng-disabled=\"pageNumber==='...'\" ng-click=\"setCurrent(pageNumber)\">{{pageNumber}}<\/md-button> <md-button aria-label=\"Last page\" ng-if=\"directionLinks\" ng-disabled=\"pagination.current===pagination.last\" ng-click=\"setCurrent(pagination.current + 1)\"> <ng-md-icon class=\"fa fa-chevron-right\"><\/ng-md-icon> <\/md-button> <md-button ng-if=\"boundaryLinks\" ng-disabled=\"pagination.current===pagination.last\" ng-click=\"setCurrent(pagination.last)\"> <ng-md-icon icon=\"last_page\"><\/ng-md-icon> <\/md-button><\/section>";

      $templateCache.put('angularUtils.directives.dirPagination.template', strVar);
    }

    function dirPaginationControlsDirective(paginationService, paginationTemplate) {

        var numberRegex = /^\d+$/;

        var DDO = {
            restrict: 'AE',
            scope: {
                maxSize: '=?',
                onPageChange: '&?',
                paginationId: '=?',
                autoHide: '=?'
            },
            link: dirPaginationControlsLinkFn
        };

        // We need to check the paginationTemplate service to see whether a template path or
        // string has been specified, and add the `template` or `templateUrl` property to
        // the DDO as appropriate. The order of priority to decide which template to use is
        // (highest priority first):
        // 1. paginationTemplate.getString()
        // 2. attrs.templateUrl
        // 3. paginationTemplate.getPath()
        var templateString = paginationTemplate.getString();
        if (templateString !== undefined) {
            DDO.template = templateString;
        } else {
            DDO.templateUrl = function(elem, attrs) {
                return attrs.templateUrl || paginationTemplate.getPath();
            };
        }
        return DDO;

        function dirPaginationControlsLinkFn(scope, element, attrs) {

            // rawId is the un-interpolated value of the pagination-id attribute. This is only important when the corresponding dir-paginate directive has
            // not yet been linked (e.g. if it is inside an ng-if block), and in that case it prevents this controls directive from assuming that there is
            // no corresponding dir-paginate directive and wrongly throwing an exception.
            var rawId = attrs.paginationId ||  DEFAULT_ID;
            var paginationId = scope.paginationId || attrs.paginationId ||  DEFAULT_ID;

            if (!paginationService.isRegistered(paginationId) && !paginationService.isRegistered(rawId)) {
                var idMessage = (paginationId !== DEFAULT_ID) ? ' (id: ' + paginationId + ') ' : ' ';
                if (window.console) {
                    console.warn('Pagination directive: the pagination controls' + idMessage + 'cannot be used without the corresponding pagination directive, which was not found at link time.');
                }
            }

            if (!scope.maxSize) { scope.maxSize = 9; }
            scope.autoHide = scope.autoHide === undefined ? true : scope.autoHide;
            scope.directionLinks = angular.isDefined(attrs.directionLinks) ? scope.$parent.$eval(attrs.directionLinks) : true;
            scope.boundaryLinks = angular.isDefined(attrs.boundaryLinks) ? scope.$parent.$eval(attrs.boundaryLinks) : false;

            var paginationRange = Math.max(scope.maxSize, 5);
            scope.pages = [];
            scope.pagination = {
                last: 1,
                current: 1
            };
            scope.range = {
                lower: 1,
                upper: 1,
                total: 1
            };

            scope.$watch('maxSize', function(val) {
                if (val) {
                    paginationRange = Math.max(scope.maxSize, 5);
                    generatePagination();
                }
            });

            scope.$watch(function() {
                if (paginationService.isRegistered(paginationId)) {
                    return (paginationService.getCollectionLength(paginationId) + 1) * paginationService.getItemsPerPage(paginationId);
                }
            }, function(length) {
                if (0 < length) {
                    generatePagination();
                }
            });

            scope.$watch(function() {
                if (paginationService.isRegistered(paginationId)) {
                    return (paginationService.getItemsPerPage(paginationId));
                }
            }, function(current, previous) {
                if (current != previous && typeof previous !== 'undefined') {
                    goToPage(scope.pagination.current);
                }
            });

            scope.$watch(function() {
                if (paginationService.isRegistered(paginationId)) {
                    return paginationService.getCurrentPage(paginationId);
                }
            }, function(currentPage, previousPage) {
                if (currentPage != previousPage) {
                    goToPage(currentPage);
                }
            });

            scope.setCurrent = function(num) {
                if (paginationService.isRegistered(paginationId) && isValidPageNumber(num)) {
                    num = parseInt(num, 10);
                    paginationService.setCurrentPage(paginationId, num);
                }
            };

            /**
             * Custom "track by" function which allows for duplicate "..." entries on long lists,
             * yet fixes the problem of wrongly-highlighted links which happens when using
             * "track by $index" - see https://github.com/michaelbromley/angularUtils/issues/153
             * @param id
             * @param index
             * @returns {string}
             */
            scope.tracker = function(id, index) {
                return id + '_' + index;
            };

            function goToPage(num) {
                if (paginationService.isRegistered(paginationId) && isValidPageNumber(num)) {
                    var oldPageNumber = scope.pagination.current;

                    scope.pages = generatePagesArray(num, paginationService.getCollectionLength(paginationId), paginationService.getItemsPerPage(paginationId), paginationRange);
                    scope.pagination.current = num;
                    updateRangeValues();

                    // if a callback has been set, then call it with the page number as the first argument
                    // and the previous page number as a second argument
                    if (scope.onPageChange) {
                        scope.onPageChange({
                            newPageNumber : num,
                            oldPageNumber : oldPageNumber
                        });
                    }
                }
            }

            function generatePagination() {
                if (paginationService.isRegistered(paginationId)) {
                    var page = parseInt(paginationService.getCurrentPage(paginationId)) || 1;
                    scope.pages = generatePagesArray(page, paginationService.getCollectionLength(paginationId), paginationService.getItemsPerPage(paginationId), paginationRange);
                    scope.pagination.current = page;
                    scope.pagination.last = scope.pages[scope.pages.length - 1];
                    if (scope.pagination.last < scope.pagination.current) {
                        scope.setCurrent(scope.pagination.last);
                    } else {
                        updateRangeValues();
                    }
                }
            }

            /**
             * This function updates the values (lower, upper, total) of the `scope.range` object, which can be used in the pagination
             * template to display the current page range, e.g. "showing 21 - 40 of 144 results";
             */
            function updateRangeValues() {
                if (paginationService.isRegistered(paginationId)) {
                    var currentPage = paginationService.getCurrentPage(paginationId),
                        itemsPerPage = paginationService.getItemsPerPage(paginationId),
                        totalItems = paginationService.getCollectionLength(paginationId);

                    scope.range.lower = (currentPage - 1) * itemsPerPage + 1;
                    scope.range.upper = Math.min(currentPage * itemsPerPage, totalItems);
                    scope.range.total = totalItems;
                }
            }
            function isValidPageNumber(num) {
                return (numberRegex.test(num) && (0 < num && num <= scope.pagination.last));
            }
        }

        /**
         * Generate an array of page numbers (or the '...' string) which is used in an ng-repeat to generate the
         * links used in pagination
         *
         * @param currentPage
         * @param rowsPerPage
         * @param paginationRange
         * @param collectionLength
         * @returns {Array}
         */
        function generatePagesArray(currentPage, collectionLength, rowsPerPage, paginationRange) {
            var pages = [];
            var totalPages = Math.ceil(collectionLength / rowsPerPage);
            var halfWay = Math.ceil(paginationRange / 2);
            var position;

            if (currentPage <= halfWay) {
                position = 'start';
            } else if (totalPages - halfWay < currentPage) {
                position = 'end';
            } else {
                position = 'middle';
            }

            var ellipsesNeeded = paginationRange < totalPages;
            var i = 1;
            while (i <= totalPages && i <= paginationRange) {
                var pageNumber = calculatePageNumber(i, currentPage, paginationRange, totalPages);

                var openingEllipsesNeeded = (i === 2 && (position === 'middle' || position === 'end'));
                var closingEllipsesNeeded = (i === paginationRange - 1 && (position === 'middle' || position === 'start'));
                if (ellipsesNeeded && (openingEllipsesNeeded || closingEllipsesNeeded)) {
                    pages.push('...');
                } else {
                    pages.push(pageNumber);
                }
                i ++;
            }
            return pages;
        }

        /**
         * Given the position in the sequence of pagination links [i], figure out what page number corresponds to that position.
         *
         * @param i
         * @param currentPage
         * @param paginationRange
         * @param totalPages
         * @returns {*}
         */
        function calculatePageNumber(i, currentPage, paginationRange, totalPages) {
            var halfWay = Math.ceil(paginationRange/2);
            if (i === paginationRange) {
                return totalPages;
            } else if (i === 1) {
                return i;
            } else if (paginationRange < totalPages) {
                if (totalPages - halfWay < currentPage) {
                    return totalPages - paginationRange + i;
                } else if (halfWay < currentPage) {
                    return currentPage - halfWay + i;
                } else {
                    return i;
                }
            } else {
                return i;
            }
        }
    }

    /**
     * This filter slices the collection into pages based on the current page number and number of items per page.
     * @param paginationService
     * @returns {Function}
     */
    function itemsPerPageFilter(paginationService) {

        return function(collection, itemsPerPage, paginationId) {
            if (typeof (paginationId) === 'undefined') {
                paginationId = DEFAULT_ID;
            }
            if (!paginationService.isRegistered(paginationId)) {
                throw 'pagination directive: the itemsPerPage id argument (id: ' + paginationId + ') does not match a registered pagination-id.';
            }
            var end;
            var start;
            if (angular.isObject(collection)) {
                itemsPerPage = parseInt(itemsPerPage) || 9999999999;
                if (paginationService.isAsyncMode(paginationId)) {
                    start = 0;
                } else {
                    start = (paginationService.getCurrentPage(paginationId) - 1) * itemsPerPage;
                }
                end = start + itemsPerPage;
                paginationService.setItemsPerPage(paginationId, itemsPerPage);

                if (collection instanceof Array) {
                    // the array just needs to be sliced
                    return collection.slice(start, end);
                } else {
                    // in the case of an object, we need to get an array of keys, slice that, then map back to
                    // the original object.
                    var slicedObject = {};
                    angular.forEach(keys(collection).slice(start, end), function(key) {
                        slicedObject[key] = collection[key];
                    });
                    return slicedObject;
                }
            } else {
                return collection;
            }
        };
    }

    /**
     * Shim for the Object.keys() method which does not exist in IE < 9
     * @param obj
     * @returns {Array}
     */
    function keys(obj) {
        if (!Object.keys) {
            var objKeys = [];
            for (var i in obj) {
                if (obj.hasOwnProperty(i)) {
                    objKeys.push(i);
                }
            }
            return objKeys;
        } else {
            return Object.keys(obj);
        }
    }

    /**
     * This service allows the various parts of the module to communicate and stay in sync.
     */
    function paginationService() {

        var instances = {};
        var lastRegisteredInstance;

        this.registerInstance = function(instanceId) {
            if (typeof instances[instanceId] === 'undefined') {
                instances[instanceId] = {
                    asyncMode: false
                };
                lastRegisteredInstance = instanceId;
            }
        };

        this.deregisterInstance = function(instanceId) {
            delete instances[instanceId];
        };

        this.isRegistered = function(instanceId) {
            return (typeof instances[instanceId] !== 'undefined');
        };

        this.getLastInstanceId = function() {
            return lastRegisteredInstance;
        };

        this.setCurrentPageParser = function(instanceId, val, scope) {
            instances[instanceId].currentPageParser = val;
            instances[instanceId].context = scope;
        };
        this.setCurrentPage = function(instanceId, val) {
            instances[instanceId].currentPageParser.assign(instances[instanceId].context, val);
        };
        this.getCurrentPage = function(instanceId) {
            var parser = instances[instanceId].currentPageParser;
            return parser ? parser(instances[instanceId].context) : 1;
        };

        this.setItemsPerPage = function(instanceId, val) {
            instances[instanceId].itemsPerPage = val;
        };
        this.getItemsPerPage = function(instanceId) {
            return instances[instanceId].itemsPerPage;
        };

        this.setCollectionLength = function(instanceId, val) {
            instances[instanceId].collectionLength = val;
        };
        this.getCollectionLength = function(instanceId) {
            return instances[instanceId].collectionLength;
        };

        this.setAsyncModeTrue = function(instanceId) {
            instances[instanceId].asyncMode = true;
        };

        this.setAsyncModeFalse = function(instanceId) {
            instances[instanceId].asyncMode = false;
        };

        this.isAsyncMode = function(instanceId) {
            return instances[instanceId].asyncMode;
        };
    }

    /**
     * This provider allows global configuration of the template path used by the dir-pagination-controls directive.
     */
    function paginationTemplateProvider() {

        var templatePath = 'angularUtils.directives.dirPagination.template';
        var templateString;

        /**
         * Set a templateUrl to be used by all instances of <dir-pagination-controls>
         * @param {String} path
         */
        this.setPath = function(path) {
            templatePath = path;
        };

        /**
         * Set a string of HTML to be used as a template by all instances
         * of <dir-pagination-controls>. If both a path *and* a string have been set,
         * the string takes precedence.
         * @param {String} str
         */
        this.setString = function(str) {
            templateString = str;
        };

        this.$get = function() {
            return {
                getPath: function() {
                    return templatePath;
                },
                getString: function() {
                    return templateString;
                }
            };
        };
    }
})();

angularAPP.controller('HealthcheckCtrl', function ($scope, $rootScope, $http, $log, env) {

  $rootScope.showList = true;

  var allClusters = env.getAllClusters();

  angular.forEach(allClusters, function(cluster) {
    $http.get(cluster.KAFKA_REST + '/topics').then(function(response) {
        var isOk = (response.status >= 200 && response.status < 400 ) ? true : false;
        cluster.isOk = isOk;
    })
  })
  $scope.allClusters = allClusters;

  //TODO healthcheck + brokers


});

angularAPP.controller('HomeCtrl', function ($scope, $rootScope, env) {
  $rootScope.showList = true;

  $scope.$on('$routeChangeSuccess', function() {
    $scope.kafkaRest = env.getSelectedCluster().KAFKA_REST;
  });

});
angularAPP.controller('ViewTopicCtrl', function ($scope, $routeParams, $rootScope, $filter, $log, $location,$cookies, $http, $base64, TopicFactory, env, $q, $timeout , consumerFactory, HttpFactory) {

  $log.debug($routeParams.topicName, "Starting [ViewTopicCtrl]");

  var topicName = $routeParams.topicName;
  var selectedTabIndex = $routeParams.selectedTabIndex;
  var topicCategoryUrl = $routeParams.topicCategoryUrl;
  var topicMenuItem = $routeParams.menuItem;

  $scope.showSpinner = true;
  $scope.showInnerSpinner = false ;

      //TODO add error messages for failed requrests + false spinner
      TopicFactory.getTopicSummary(topicName, $scope.cluster.KAFKA_REST)
      .then(function success(topic){
            topic.data.configs = makeConfigsArray(topic.data.configs);
            $scope.topic = topic.data;
            if($scope.topic.partitions.length == 1) {
              $scope.selectedPartition = '0'
              $scope.disableAllPartitionsOption = true;
            }

            $scope.showAdvanced = ($scope.topic.partitions.length == 1 ? true : false )
            $scope.disableAllPartitionButtons = ($scope.topic.partitions.length == 1 ? true : false )
      },
     function failure(responseError2) {
     });

    TopicFactory.getAllTopics($scope.cluster.KAFKA_REST) //TODO do we need this?
    .then(function success(allTopics){
      $scope.allTopics = allTopics;
    });

  $scope.disableAllPartitionButtons = false;

/*******************************
 * topic-toolbar.html
********************************/

  $scope.showDownloadDiv = false;

  $scope.toggleList = function () {
     $rootScope.showList = !$rootScope.showList;
  };
  $scope.refreshDataForDownload = function(searchFilter){
      $scope.dataForDownload = $filter('filter')($scope.rows, searchFilter)
  }

  $scope.downloadData = function (topicName) {
    $log.info("Download requested for " + $scope.dataForDownload.length + " bytes ");
    var json = JSON.stringify($scope.dataForDownload);
    var blob = new Blob([json], {type: "application/json;charset=utf-8;"});
    var downloadLink = angular.element('<a></a>');
    downloadLink.attr('href', window.URL.createObjectURL(blob));
    downloadLink.attr('download', topicName + '.json');
    downloadLink[0].click();
  };

/*******************************
 * AUTO COMPLETE
********************************/
  $scope.simulateQuery = false;

  $scope.querySearch = function querySearch (query) {
    var results = query ? $scope.allTopics.filter( createFilterFor(query) ) : $scope.allTopics,
        deferred;
    if ($scope.simulateQuery) {
      deferred = $q.defer();
      $timeout(function () { deferred.resolve( results ); }, Math.random() * 1000, false);
      return deferred.promise;
    } else {
      return results;
    }
  }
  $scope.goTo = function goTo (topic) {
   var urlType = (topic.isControlTopic == true) ? 'c' : 'n';
    $location.path ("cluster/"+ $scope.cluster.NAME + "/topic/" +  urlType + "/" + topic.topicName);
  }
  function createFilterFor(query) {
    var lowercaseQuery = angular.lowercase(query);

    return function filterFn(item) {
      return (item.topicName.indexOf(lowercaseQuery) === 0);
    };

  }

/*******************************
 * topic-configuration.html
********************************/

  $scope.showMoreDesc = [];

  $scope.toggleMoreDesc = function (index) {
      $scope.showMoreDesc[index] = !$scope.showMoreDesc[index];
  };

  function makeConfigsArray(configs) {
//    configs = {"segment.bytes":"104857600","cleanup.policy":"compact","compression.type":"producer"};
    var configArray = [];

    angular.forEach(configs, function(value, key) {
          var object = {
            configuration : key,
            value : value,
            defaultValue : getDefaultConfigValue(key),
            documentation : getConfigDescription(key)
          };
          this.push(object);
    }, configArray);
    return configArray;
  }

/*******************************
 * topic data / Tabs handling
********************************/

  $scope.selectedTabNnumber = setSelectedDataTab(selectedTabIndex);

  function setSelectedDataTab(selectedTabIndex) {
    switch(selectedTabIndex) {
        case "topic": return 0;
        case "table": return 1;
        case "rawdata": return 2;
        default: return 0;
    }
  }

  $scope.selectedMenuItem = (topicMenuItem != undefined) ? topicMenuItem : 'data';

  $scope.setMenuSelection = function(currentMenuItem, cluster) {
        $scope.selectedMenuItem = currentMenuItem;
        $location.path("cluster/"+ cluster.NAME + "/topic/" +  topicCategoryUrl + "/" + topicName + "/" + currentMenuItem, false);
  }

  $scope.onTabChanges = function(currentTabIndex, cluster){
      $location.path ("cluster/"+ cluster.NAME + "/topic/" +  topicCategoryUrl + "/" + topicName +  "/" + $scope.selectedMenuItem + "/" + currentTabIndex, false);
  };

  $scope.maxHeight = window.innerHeight - 300;
  if ($scope.maxHeight < 310) {$scope.maxHeight = 310}

/*******************************
 * DATA stuff
********************************/
   $scope.partitionIsEmpty = false;
   $scope.seekToEnd = false;
   $scope.selectedPartition = "-1";

  function setTopicMessages(allData, format, forPartition) {
    if(forPartition) {
        $scope.showAdvanced = true;
        $scope.disableAllPartitionButtons = true;
        if(allData.length === 0) $scope.partitionIsEmpty = true;
    }
     $scope.rows = allData;
     $scope.format=format;
     $scope.dataForDownload = $scope.rows

     if(format == 'binary'){
       angular.forEach($scope.rows, function(row){
          row.key=$base64.decode(row.key)
          row.value=$base64.decode(row.value)
       })
      $scope.dataForDownload = $scope.rows
     }
     $scope.showSpinner = false;

    if(allData.length > 0) {

     var floor = $scope.firstOffsetForPartition ? $scope.firstOffsetForPartition : allData[0].offset;
     }
  }

  function getDeserializationErrorMessage(reason, type) {
      return $log.debug('Failed with '+ type +' type :(  (' + reason + ')');
  }

  createAndFetch(consumerFactory.getConsumerType(topicName), topicName);
  $scope.hideTab = false;

  function createAndFetch(format, topicName) {
    $scope.showInnerSpinner = true;
    $log.debug("... DATA FOR PARTITION [ ALL ]...");
    $scope.uuid = consumerFactory.genUUID();
    consumerFactory
        .createConsumer(format, topicName, $scope.uuid)
        .then(function(res){
            return consumerFactory.getConsumer(format, $scope.uuid);
        })
        .then(function(consumer) {
            consumerFactory.getDataFromBeginning(consumer, format, topicName).then(function (allData) {
                if(allData === -1) {
                    $log.debug(topicName, "FAILED TO GET DATA, NEED TO RETRY", allData, consumer, topicName);
                    createAndFetch(consumerFactory.getConsumerTypeRetry(format, topicName), topicName);
                    $scope.showInnerSpinner = false;
                } else {
                    $log.debug(topicName, "GOT DATA, WILL RENDER", " [", allData.data.length, "] [", format, "] MESSAGES");
                    setTopicMessages(allData.data, format, false)
                    $scope.showInnerSpinner = false;
                    $scope.seekToEnd = false;
                }
            });
        });
  }
    function createAndFetchFromEnd(format, topicName) {
    $scope.showInnerSpinner = true;
    $log.debug("... DATA FOR PARTITION [ ALL ]...");
    $scope.uuid = consumerFactory.genUUID();
    consumerFactory
        .createConsumer(format, topicName, $scope.uuid)
        .then(function(res){
            return consumerFactory.getConsumer(format, $scope.uuid);
        })
        .then(function(consumer) {
            consumerFactory.getDataFromEnd(consumer, format, topicName).then(function (allData) {
                if(allData === -1) {
                    $log.debug(topicName, "FAILED TO GET DATA, NEED TO RETRY", allData, consumer, topicName);
                    createAndFetchFromEnd(consumerFactory.getConsumerTypeRetry(format, topicName), topicName);
                    $scope.showInnerSpinner = false;
                } else {
                    $log.debug(topicName, "GOT DATA, WILL RENDER", " [", allData.data.length, "] [", format, "] MESSAGES");
                    setTopicMessages(allData.data, format, false)
                    $scope.showInnerSpinner = false;
                    $scope.seekToEnd = true;
                }
            });
        });
  }

  $scope.createAndFetchFromEnd = function(format, topicName) {
    createAndFetchFromEnd(format, topicName)
  }
  $scope.createAndFetch = function(format, topicName) {
    createAndFetch(format, topicName)
  }
  $scope.selectedOffset = {offset: 0}

  $scope.assignPartitions = function assignPartitions(partition, offset, position, firstTime) {
    $scope.selectedPartition = partition;
    $scope.showInnerSpinner = true

    $log.debug("... DATA FOR PARTITION [" + partition + "]...", position);
    var format = consumerFactory.getConsumerType(topicName);//$scope.format; //TODO

    //TODO If partitions = all (somehow) then createAndFetch
    if(partition == -1) {
        $scope.showAdvanced = false;
        $scope.disableAllPartitionButtons = false;
        createAndFetch(format, topicName);
        return;
    }

    if(position=='end'){
      $scope.seekToEnd=true;
    } else {
      $scope.seekToEnd=false;
    }


    //TODO make a loading for data only for the case partition is empty// $scope.showSpinner = true;
    var partition = [ { "partition" : partition } ]; //create array because assignments works for all too.

    offset = parseInt(offset);
    if (!angular.isDefined(offset)){offset = 1}
    $scope.selectedOffset.offset=offset;
    $scope.uuid = consumerFactory.genUUID();

    consumerFactory
        .createConsumer(format, topicName, $scope.uuid)
        .then(function(res){
            return consumerFactory.getConsumer(format, $scope.uuid);
        })
        .then(function(consumer) {
            $log.debug(topicName, "1) GOT PARTITION", partition);
            consumerFactory.getDataForPartition(topicName, consumer, format, partition, offset, position)
            .then(function(allData) {
                if(allData !== -1) {
                    if(firstTime && allData.data.length !== 0) { $scope.firstOffsetForPartition = allData.data[0].offset }
                    setTopicMessages(allData.data, format, true);
                    $scope.showInnerSpinner = false;

                } else {
                    $scope.cannotGetDataForPartition = "Cannot get data for partition [" + partitions + "]. Please refresh."
                    $scope.showInnerSpinner = false;
                }
            });
        });
  }

  function getDefaultConfigValue(configKey) {
    var defaultConfigValue = "";
    angular.forEach(KAFKA_DEFAULTS, function (kafkaDefault) {
      if (kafkaDefault.property == configKey) {
        defaultConfigValue = kafkaDefault.default;
      }
    });
    return defaultConfigValue;
  };

  function getConfigDescription(configKey) {
    var configDescription = "";
    angular.forEach(KAFKA_DEFAULTS, function (kafkaDefault) {
      if (kafkaDefault.property == configKey) {
        configDescription = kafkaDefault.description;
      }
    });
    return configDescription;
  };

});

angularAPP.factory('TopicFactory', function (HttpFactory) {
    var defaultContentType = 'application/vnd.kafka.avro.v2+json';

    return {
          getTopicSummary: function (topicName, endpoint) {
             return HttpFactory.req('GET', endpoint  + '/topics/' + topicName);
          },
          getAllTopics: function(endpoint) {
            return HttpFactory.req('GET', endpoint + "/topics")
          }
    }
});

var totalBrokersModule = angular.module('totalBrokers', ["HttpFactory"]);

totalBrokersModule.directive('totalBrokers', function(templates) {
  return {
    restrict: 'E',
    templateUrl: 'src/kafka-topics/dashboard-components/total-brokers/total-brokers.html',
    controller: 'TotalBrokersCtrl'
  };
});

totalBrokersModule.factory('BrokersBackendFactory', function (HttpFactory) {
    return {
        getBrokers: function (endpoint) {
           return HttpFactory.req('GET', endpoint + '/brokers');
        }
    }
});

totalBrokersModule.controller('TotalBrokersCtrl', function ($scope,  $log, BrokersBackendFactory, env) {
var endpoint = env.KAFKA_REST().trim()
    BrokersBackendFactory.getBrokers(endpoint).then(
      function success(brokers) {
       $scope.totalBrokers = brokers.data.brokers.length;
      },
      function failure() {
        $scope.connectionFailure = true;
    });

});

var totalTopicsModule = angular.module('totalTopics', ["HttpFactory"]);

totalTopicsModule.directive('totalTopics', function(templates) {
  return {
    restrict: 'E',
    templateUrl: 'src/kafka-topics/dashboard-components/total-topics/total-topics.html',
    controller: 'TotalTopicsCtrl'
  };
});

totalTopicsModule.factory('TopicsCountBackendFactory', function (HttpFactory) {
    return {
        getTopics: function (endpoint) {
           return HttpFactory.req('GET', endpoint + '/topics');
        }
    }
});

totalTopicsModule.controller('TotalTopicsCtrl', function ($scope, TopicsCountBackendFactory, env) {
    TopicsCountBackendFactory.getTopics(env.KAFKA_REST()).then(function(data) {
      $scope.totalTopics = data.data.length
    })
});
/**

 This module gives a list of topics with some metadata.

 Example of usage:
     <topics-list mode="normal" cluster="{{cluster}}"></topics-list>

     mode can be `normal`, `system` or `compact`
     cluster is a scope var in module's controller and expects the selected cluster to pick up topics from.

**/
var topicsListModule = angular.module('topicsList', ["HttpFactory"]);

topicsListModule.directive('topicsList', function(templates) {
  return {
    restrict: 'E',
    templateUrl: function($elem, $attr){
      return templates[$attr.mode];
    },
    controller: 'KafkaTopicsListCtrl'
  };
});

topicsListModule.factory('templates', function() {
  return {
    compact: 'src/kafka-topics/list/compact-topics-list.html',
    home:  'src/kafka-topics/list/topics-list.html',
  };
});

topicsListModule.factory('TopicsListFactory', function (HttpFactory) {
    return {
        getTopics: function (endpoint) {
           return HttpFactory.req('GET', endpoint + '/topics');
        },
        getTopicDetails: function(topicName, endpoint){
           return HttpFactory.req('GET', endpoint + '/topics' + '/' + topicName )
        },
        sortByKey: function (array, key, reverse) {
          return sortByKey(array, key, reverse);
        }
    }
    function sortByKey(array, key, reverse) {
        return array.sort(function (a, b) {
          var x = a[key];
          var y = b[key];
          return ((x < y) ? -1 * reverse : ((x > y) ? 1 * reverse : 0));
        });
    }
});

topicsListModule.factory('shortList', function (HttpFactory) {
  return {
    sortByKey: function (array, key, reverse) {
    return sortByKey(array, key, reverse);
    }
  }
  function sortByKey(array, key, reverse) {
    return array.sort(function (a, b) {
      var x = a[key];
      var y = b[key];
      return ((x < y) ? -1 * reverse : ((x > y) ? 1 * reverse : 0));
    });
  }
})

topicsListModule.controller('KafkaTopicsListCtrl', function ($scope, $location, $rootScope, $routeParams, $cookies, $filter, $log, $q, $http, TopicsListFactory, shortList, consumerFactory) {
  $rootScope.showList = true;

  $scope.topic = $routeParams.topicName


  var schemas;
  $scope.displayingControlTopics = false;
  $scope.$watch(
    function () { return $routeParams.topicName },
    function () { if(angular.isDefined($routeParams.topicName)) {
      $scope.topicName = $routeParams.topicName
      $scope.displayingControlTopics = checkIsControlTopic($scope.topicName);
    }
 },
   true);

  $scope.$watch(
    function () { return $scope.cluster; },
    function () { if(typeof $scope.cluster == 'object'){
      getLeftListTopics();
      loadSchemas()
    } },
   true);

  $scope.shortenControlCenterName = function (topic) {
    return shortenControlCenterName(topic);
  }

  $scope.query = { order: '-totalMessages', limit: 100, page: 1 };

  // This one is called each time - the user clicks on an md-table header (applies sorting)
  $scope.logOrder = function (a) {
      sortTopics(a);
  };

  $scope.totalMessages = function (topic) {
    if(topic.totalMessages == 0) return '0';
    var sizes = ['', 'K', 'M', 'B', 'T', 'Quan', 'Quin'];
    var i = +Math.floor(Math.log(topic.totalMessages) / Math.log(1000));
    return (topic.totalMessages / Math.pow(1000, i)).toFixed(i ? 1 : 0) + sizes[i];
  }

  $scope.selectTopicList = function (displayingControlTopics) {
    $scope.selectedTopics = $scope.topics.filter(function(el) {return el.isControlTopic == $scope.displayingControlTopics})
  }


  var itemsPerPage = (window.innerHeight - 280) / 48;
  Math.floor(itemsPerPage) < 3 ? $scope.topicsPerPage =3 : $scope.topicsPerPage = Math.floor(itemsPerPage);

  $scope.listClick = function (topicName, isControlTopic) {
    var urlType = (isControlTopic == true) ? 'c' : 'n';
    $location.path("cluster/" + $scope.cluster.NAME + "/topic/" + urlType + "/" + topicName, true);
  }

  function getLeftListTopics() {
    $scope.selectedTopics = [];
    $scope.topics = [];
    TopicsListFactory.getTopics($scope.cluster.KAFKA_REST.trim()).then(function (allData){
        var topics = [];
        angular.forEach(allData.data, function(topic, key) {
            TopicsListFactory.getTopicDetails(topic, $scope.cluster.KAFKA_REST.trim()).then(function(res){
                var configsCounter = 0;
                angular.forEach(res.data.configs, function(value, key) { configsCounter++;});
                var topicImproved = {
                    topicName : res.data.name,
                    partitions : res.data.partitions.length,
                    replication : res.data.partitions[0].replicas.length,
                    customConfig : configsCounter,
                    isControlTopic : checkIsControlTopic(res.data.name)
                }

                topics.push(topicImproved);
               if (topics.length == allData.data.length) {
                  $scope.topics = topics;
                  $scope.selectedTopics = topics.filter(function(el) {return el.isControlTopic == $scope.displayingControlTopics});
                  console.log('Total topics fetched:', allData.data.length)
                  console.log('Length of improved topic array:', topics.length)
                  console.log('Selected topics(listed):', $scope.selectedTopics.length)

                  $scope.topicsIndex = arrayObjectIndexOf($scope.selectedTopics, $routeParams.topicName, 'topicName' ) + 1;
                  $scope.topicsPage = Math.ceil($scope.topicsIndex / $scope.topicsPerPage);

                  if ($scope.topicsPage < 1) {
                    $scope.topicsPage = 1
                  }
               }
            })

        })

        //$scope.selectTopicList(true);

    })
  }

  function sortTopics(type) {
      var reverse = 1;
      if (type.indexOf('-') == 0) {
        // remove the - symbol
        type = type.substring(1, type.length);
        reverse = -1;
      }
      $scope.selectedTopics = shortList.sortByKey($scope.selectedTopics, type, reverse);
  }


function arrayObjectIndexOf(myArray, searchTerm, property) {
    for(var i = 0, len = myArray.length; i < len; i++) {
        if (myArray[i][property] === searchTerm) return i;
    }
    return -1;
}

  function shortenControlCenterName(topic) {
      if (topic.isControlTopic) {
        return topic.topicName
          .replace('_confluent-controlcenter-0-', '...')
          // .replace('aggregate-topic-partition', 'aggregate-topic')
          .replace('MonitoringMessageAggregatorWindows', 'monitor-msg')
          .replace('connect-configs', 'monitor-msg')
          .replace('aggregatedTopicPartitionTableWindows', 'aggregate-window')
          .replace('monitoring-aggregate-rekey', 'monitor-rekey')
          .replace('MonitoringStream', 'monitor-stream')
          .replace('MonitoringVerifierStore', 'monitor-verifier')
          .replace('...Group', '...group')
          .replace('FIFTEEN_SECONDS', '15sec')
          .replace('ONE_HOUR', '1hour')
          .replace('ONE_WEEK', '1week');
      } else {
        return topic.topicName;
      }
  }


  function loadSchemas(){
    var uuid=consumerFactory.genUUID();
    consumerFactory.createConsumer('json', '_schemas', uuid).then( function (response) {
      if (response.status == 409 || response.status == 200) {

        var consumer = {group :'kafka_topics_ui_json_' + uuid, instance: 'kafka-topics-ui-json' };
           consumerFactory.getDataFromBeginning(consumer,'json', '_schemas').then(function (allSchemas) {
             $rootScope.schemas = allSchemas;
             schemas = allSchemas
             return schemas
           })
        if (response.status == 409) {
            var msg = response.data.message;
            msg = "Conflict 409. " + msg;
            $log.warn(msg)
         }
       } else {
        $log.warn(response.data.message)
       }
    })
  }


  function getDataType (topicName) {
    var dataType = "...";
    var dataType_key;
    var dataType_value;
    // Check if we know the topic data type a priory
    if (KNOWN_TOPICS.JSON_TOPICS && KNOWN_TOPICS.JSON_TOPICS.indexOf(topicName) > -1) {
      dataType = "json";
    } else if (KNOWN_TOPICS.BINARY_TOPICS && KNOWN_TOPICS.BINARY_TOPICS.indexOf(topicName.substring(0, 24)) > -1) {
      dataType = "binary";
    } else {
      // If topicDetails are not available wait
          if (schemas) {
          angular.forEach(angular.fromJson(schemas.data), function (schema) {
            if ((schema.value != null) && (schema.value.subject != null) && (schema.value.subject == topicName + "-value")) {
              //$log.info("FOUND YOU !! " + topicName);
              dataType_value = "avro";
            }
            if ((schema.value != null) && (schema.value.subject != null) && (schema.value.subject == topicName + "-key")) {
              //$log.info("FOUND YOU !! " + topicName);
              dataType_key = "avro";
            }
          });
          if (dataType_value=="avro" && dataType_key=="avro") {
            dataType="avro";
          }
}
    }
    if (dataType == "") {
      $log.warn("Could not find the message type of topic [" + topicName + "]");
    }
    return dataType;
  }

   $scope.getDataType = function (topicName) {
      return getDataType(topicName);
    };

    function checkIsControlTopic(topicName) {
      var isControlTopic = false;
      angular.forEach(KNOWN_TOPICS.CONTROL_TOPICS, function (controlTopicPrefix) {
        if (topicName.lastIndexOf(controlTopicPrefix, 0) === 0)
          isControlTopic = true;
      });
      return isControlTopic;
    }

});

var dataFlatTableModule = angular.module('flatView', []);

dataFlatTableModule.directive('flatView', function() {
  return {
    restrict: 'E',
    scope: {
      data: '=',
      partitions: '=',
      search: '=',
      topic: '='
    },
    templateUrl: 'src/kafka-topics/view/templates/data/flatten/data-flatten-view.html',
    controller: 'dataFlatTableCtrl',
   link: function(scope, element, attrs){
//         scope.$watch(function() {
//            console.log('test', scope.data)
//            scope.mrows = scope.data
//          });
    }
  };
});

topicsListModule.factory('FlatTableFactory', function (HttpFactory) {

    return {
        flattenObject: function (ob) {
           return flattenObject(ob);
        },
        sortByKey: function (array, key, reverse) {
            return sortByKey(array, key, reverse);
          },
        getTopicSummary: function (topicName, endpoint) {
           return HttpFactory.req('GET', endpoint  + '/topics/summary/' + topicName);
        }
    }

    function flattenObject(ob) {
        var toReturn = {};

        for (var i in ob) {
            if (!ob.hasOwnProperty(i)) continue;

            if ((typeof ob[i]) == 'object') {
                var flatObject = flattenObject(ob[i]);

                for (var x in flatObject) {
                    if (!flatObject.hasOwnProperty(x)) continue;
                    toReturn[i + '.' + x] = flatObject[x];
                }

            } else {
                toReturn[i] = ob[i];
            }
        }
        return toReturn;
    };

     // Sort arrays by key
     function sortByKey(array, key, reverse) {
          return array.sort(function (a, b) {
            var x = a[key];
            var y = b[key];
            return ((x < y) ? -1 * reverse : ((x > y) ? 1 * reverse : 0));
          });
     }
});

//TODO Clean me up! ALL shit happens here
dataFlatTableModule.controller('dataFlatTableCtrl', function ($scope, $log, $routeParams, $filter, FlatTableFactory, env, hotRegisterer) {

 $scope.maxHeight = window.innerHeight - 310;
    if ($scope.maxHeight < 310) {$scope.maxHeight = 310}


   $scope.$watch("data", function() {
        if($scope.data) {
            flattenTable($scope.data); // because data is async/ly coming from an http call, we need to watch it, directive gets compiled from the beginning.
        }
   })


   $scope.$watch("search", function(newValue) {
    if($scope.data){
    $scope.refreshData()
    }
   })


  var t =0;

  $scope.$parent.$parent.$parent.$parent.$watch("showList",function() {
    if (t !=0 ) {
      setTimeout(function () {
        $scope.$apply(function () {
         createHotTable();
        });
      })
    }
  t++
  })


  var doFlattenValue;
  var doFlattenKey;
  var doNotFlatten;

   $scope.$watch("topic", function() {
        if($scope.topic) {
            doFlattenValue = isAvroOrJsonValue($scope.topic.valueType)
            doFlattenKey =   isAvroOrJsonValue($scope.topic.keyType)
            doNotFlatten = !(doFlattenValue || doFlattenKey);
        }
   })

  function  isAvroOrJsonValue(keyOrValue) {
     return keyOrValue=='json' || keyOrValue=='avro';
  }

  $scope.isNotAvroOrJsonValue
  $scope.paginationItems = 20;

  $scope.selectedCols = {};

  $scope.checkAndHide = function checkAndHide(name) {
    if ($scope.selectedCols.searchText){
        var showCol = $scope.selectedCols.searchText.some(function (selectedCols) {
          return selectedCols === name;
        });
        return showCol
    }
  }

  $scope.addColumnClass = function (columnIndex) {
      columnIndex = columnIndex + 1;
      var columnClass = '';
      if (columnIndex == 1 ) {columnClass='offset'}
      else if(columnIndex == 2) {columnClass='partition'}
      else if(columnIndex < 4 + $scope.keyFlatColumns.length ) {columnClass='key'}
      else if(columnIndex < 5 + $scope.keyFlatColumns.length  + $scope.valueFlatColumns.length ) {columnClass='value'}
      return columnClass;
  }

  $scope.query = { order: 'offset', limit: 100, page: 1 };

  // This one is called each time - the user clicks on an md-table header (applies sorting)
  $scope.logOrder = function (a) {
      // $log.info("Ordering event " + a);
      sortTopic(a);
  };


 function flattenTable(rows) {

         var extraColumnsNumberValue = 0;
         var extraColumnsNumberKey = 0;
         var rowWithMoreColumns;
         $scope.flatRows = [];
         if (rows.length > 0) {
             angular.forEach(rows, function ( row, key) {
             row= {
               'offset' : row.offset,
               'partition': row.partition,
               'key' : row.key,
               'value' : row.value
             }
                   if (row.key == undefined || row.key == null) row.key = '';
                   if (row.value == undefined || row.value == null) row.value = '';

                   if((angular.isNumber(row.value) || angular.isString(row.value)) && (angular.isNumber(row.key) || angular.isString(row.key))) {
                         extraColumnsNumberValue = 0
                         extraColumnsNumberKey = 0
                         var newRow = {
                             "offset" : row.offset,
                             "partition" : row.partition,
                             "key" : row.key,
                             "value" : row.value
                         }
                         $scope.cols = Object.keys(FlatTableFactory.flattenObject(newRow));
                         $scope.cols2 = [];
                         $scope.cols3 = [];
                   } else {
                         var flatValue = FlatTableFactory.flattenObject(row.value);
                         var flatKey = FlatTableFactory.flattenObject(row.key);
                         var rowExtraColumnsValues = (!(angular.isNumber(row.value) || angular.isString(row.value))) ? Object.keys(flatValue).length : 0;
                         var rowExtraColumnsKeys = (!(angular.isNumber(row.key) || angular.isString(row.key))) ? Object.keys(flatKey).length : 0;

                         if(extraColumnsNumberValue < rowExtraColumnsValues) {
                             extraColumnsNumberValue = rowExtraColumnsValues;
                             rowWithMoreColumns = row;
                         }

                         if(extraColumnsNumberKey < rowExtraColumnsKeys) {
                             extraColumnsNumberKey = rowExtraColumnsKeys;
                             rowWithMoreColumns = row;
                         }

                         var newRow = {
                             "offset" : rowWithMoreColumns.offset,
                             "partition" : rowWithMoreColumns.partition,
                             "key" : rowWithMoreColumns.key,
                             "value" : rowWithMoreColumns.value
                         }

                         $scope.cols =  Object.keys(FlatTableFactory.flattenObject(newRow));
                         if (!(angular.isNumber(row.value) || angular.isString(row.value))){
                           $scope.cols2 = Object.keys(FlatTableFactory.flattenObject(newRow.value));
                         }
                         else {
                           $scope.cols2 = []
                         }
                         if (!(angular.isNumber(row.key) || angular.isString(row.key))){
                           $scope.cols3 = Object.keys(FlatTableFactory.flattenObject(newRow.key));
                         }
                         else {
                           $scope.cols3 = [];
                         }

                   }
                   $scope.flatRows.push(FlatTableFactory.flattenObject(row));

                   if (key == rows.length -1) {
                       setTimeout(function () {
                               $scope.$apply(function () {
                                  createHotTable()
                               });
                     }, 500)
                   }
                 });

                 $scope.extraColsNumValues = extraColumnsNumberValue;
                 $scope.extraColsNumKeys = extraColumnsNumberKey;


          var itemsPerPage = (window.innerHeight - 300) / 31
          Math.floor(itemsPerPage) < 10 ? $scope.fittingItems =10 : $scope.fittingItems = Math.floor(itemsPerPage);

        $scope.paginationItems = $scope.fittingItems;
        $scope.showHideAllButtonLabel = 'show ' + rows.length;

      }
 }

  var hotRows;
  function createHotTable(){
     hotRows = [];
     $scope.hotTableHeaders = [];

      $scope.hotTableHeaders.push('Offset', 'Partition')

      if ($scope.extraColsNumKeys > 0){
        angular.forEach($scope.cols3, function(colheader) {
          $scope.hotTableHeaders.push('key.'+colheader)
        })
      } else {
        $scope.hotTableHeaders.push('Key')
      }

      if ($scope.extraColsNumValues > 0){
        angular.forEach($scope.cols2, function(colheader) {
          $scope.hotTableHeaders.push('value.'+colheader)
        })
      } else {
          $scope.hotTableHeaders.push('Value')
      }

      angular.forEach($scope.flatRows, function (rows) {
        var hotCol = [];
        angular.forEach(rows, function (col, key) {
          if( key !== "$$hashKey" ) {
           hotCol.push(col)
          }
        })

        hotRows.push(hotCol)
      })
      $scope.refreshData();
    }

   $scope.refreshData = function() {
    $scope.hotRows = $filter('filter')(hotRows, $scope.search);
     var hotsinstance = hotRegisterer.getInstance('my-handsontable');

     hotsinstance.addHook('afterRender', function () {
     $scope.hotsWidth = 25;
       angular.forEach($scope.hotTableHeaders, function (value, key) {
        $scope.hotsWidth = $scope.hotsWidth + hotsinstance.getColWidth(key);
       })
     });
   };


  function sortTopic(type) {
      var reverse = 1;
      if (type.indexOf('-') == 0) {
        // remove the - symbol
        type = type.substring(1, type.length);
        reverse = -1;
      }
       $log.info(type + " " + reverse);
      $scope.flatRows = FlatTableFactory.sortByKey($scope.flatRows, type, reverse);
  }

});


var dataRawViewModule = angular.module('rawView', []);

dataRawViewModule.directive('rawView', function() {
  return {
    restrict: 'E',
    scope: {
      data: '=',
      topicType: '='
    },
    templateUrl: 'src/kafka-topics/view/templates/data/raw/data-raw-view.html',
    controller: 'dataRawViewCtrl'
//    link: function(scope, element, attrs){ //... }
  };
});


dataRawViewModule.controller('dataRawViewCtrl', function ($scope, $log) {

   $scope.$watch("data", function() {
        if($scope.data) {
            ($scope.topicType == 'json') ? $scope.aceString = $scope.data :$scope.aceString = angular.toJson($scope.data, true);
        }
   })

   $scope.aceLoaded = function (_editor) {
        $scope.editor = _editor;
        $scope.editor.$blockScrolling = Infinity;
        _editor.setOptions({
          minLines: 33
          });
   };

  $scope.aceHeight = window.innerHeight - 290;
  $scope.aceHeight < 400 ? $scope.aceHeight = 400 : ''



});


var dataTreeViewModule = angular.module('treeView', []);

dataTreeViewModule.directive('treeView', function() {
  return {
    restrict: 'E',
    scope: {
      data: '=',
      partitions: '=', //Pass pagination?
      topic: '=', //Pass pagination?
      search: '='
    },
    templateUrl: 'src/kafka-topics/view/templates/data/tree/data-tree-view.html',
    controller: 'dataTreeViewCtrl'
  };
});


dataTreeViewModule.controller('dataTreeViewCtrl', function ($scope, $log, $base64) {

   $scope.$watch("data", function() {
        if($scope.data) {
            $scope.rows = $scope.data; // because data is async/ly coming from an http call, we need to watch it, directive gets compiled from the beginning.
            $scope.currentPage = 1
        }
   })



   $scope.paginationItems = Math.floor($scope.$parent.maxHeight / 65);
      $scope.$parent.$watch("format", function() {
           if($scope.$parent.format) {
               $scope.format = $scope.$parent.format

           }
      })
   $scope.decode = function(string){
   return $base64.decode(string)
   }

   $scope.isAvroOrJsonValue = function (keyOrValue) {
      return keyOrValue=='json' || keyOrValue=='avro';
   }


});
