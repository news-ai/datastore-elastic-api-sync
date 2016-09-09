'use strict';

var elasticsearch = require('elasticsearch');
var Q = require('q');

// Instantiate a datastore client
var datastore = require('@google-cloud/datastore')({
    projectId: 'newsai-1166'
});

// Instantiate a elasticsearch client
var client = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

/**
 * Gets a Datastore key from the kind/key pair in the request.
 *
 * @param {Object} requestData Cloud Function request data.
 * @param {string} requestData.Id Datastore ID string.
 * @returns {Object} Datastore key object.
 */
function getKeyFromRequestData(requestData, resouceType) {
    if (!requestData.Id) {
        throw new Error('Id not provided. Make sure you have a "Id" property ' +
            'in your request');
    }

    var listId = parseInt(requestData.Id, 10);
    return datastore.key([resouceType, listId]);
}

/**
 * Retrieves a record.
 *
 * @example
 * gcloud alpha functions call ds-get --data '{"kind":"gcf-test","key":"foobar"}'
 *
 * @param {Object} context Cloud Function context.
 * @param {Function} context.success Success callback.
 * @param {Function} context.failure Failure callback.
 * @param {Object} data Request data, in this case an object provided by the user.
 * @param {string} data.kind The Datastore kind of the data to retrieve, e.g. "user".
 * @param {string} data.key Key at which to retrieve the data, e.g. 5075192766267392.
 */
function getDatastore(data, resouceType) {
    var deferred = Q.defer();
    try {
        var key = getKeyFromRequestData(data, resouceType);

        datastore.get(key, function(err, entity) {
            if (err) {
                console.error(err);
                deferred.reject(new Error(error));
            }

            // The get operation will not fail for a non-existent entity, it just
            // returns null.
            if (!entity) {
                var error = 'Entity does not exist';
                console.error(error);
                deferred.reject(new Error(error));
            }

            deferred.resolve(entity);
        });

    } catch (err) {
        console.error(err);
        deferred.reject(new Error(error));
    }

    return deferred.promise;
}

function mediaListToContactMap(mediaListId) {
    var deferred = Q.defer();

    getDatastore(mediaListId, 'MediaList').then(function(mediaList) {
        if ('Contacts' in mediaList.data) {
            var mediaListContactToHashMap = {};
            for (var i = mediaList.data.Contacts.length - 1; i >= 0; i--) {
                mediaListContactToHashMap[mediaList.data.Contacts[i]] = true;
            }
            deferred.resolve(mediaListContactToHashMap);
        }
    });

    return deferred.promise;
}

function getElasticContactsByListId(listId) {
    var deferred = Q.defer();

    client.search({
        index: 'contacts',
        type: 'contact',
        size: 10000,
        body: {
            query: {
                match: {
                    'data.ListId': listId
                }
            }
        }
    }).then(function(resp) {
        var hits = resp.hits.hits;
        var contactIdToElasticId = {};

        for (var i = hits.length - 1; i >= 0; i--) {
            contactIdToElasticId[hits[i]._source.data.Id] = hits[i]._id;
        }

        deferred.resolve(contactIdToElasticId);
    }, function(err) {
        console.trace(err.message);
        deferred.reject(new Error(err.message));
    });

    return deferred.promise;
}

function syncList(data) {
    var deferred = Q.defer();
    mediaListToContactMap(data).then(function(mediaListContactToHashMap) {
        getElasticContactsByListId(data.Id).then(function (elasticContactList) {
            var contactsToDelete = [];
            console.log(mediaListContactToHashMap);
            console.log(elasticContactList);
        }, function (error) {
            deferred.reject(new Error(error));
            throw new Error(error);
        });
    }, function(error) {
        deferred.reject(new Error(error));
        throw new Error(error);
    });
    return deferred.promise;
}

/**
 * Triggered from a message on a Pub/Sub topic.
 *
 * @param {Object} context Cloud Function context.
 * @param {Function} context.success Success callback.
 * @param {Function} context.failure Failure callback.
 * @param {Object} data Request data, in this case an object provided by the Pub/Sub trigger.
 * @param {Object} data.message Message that was published via Pub/Sub.
 */
exports.syncLists = function syncLists(data) {
    return syncList(data);
};

function testSync(data) {
    return syncList(data);
};

testSync({Id: '5641762471149568'})
