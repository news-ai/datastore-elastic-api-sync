'use strict';

var raven = require('raven');
var elasticsearch = require('elasticsearch');
var Q = require('q');

// Instantiate a datastore client
var datastore = require('@google-cloud/datastore')({
    projectId: 'newsai-1166'
});

// Instantiate a elasticsearch client
var client = new elasticsearch.Client({
    host: 'https://newsai:XkJRNRx2EGCd6@search1.newsai.org',
    // log: 'trace',
    rejectUnauthorized: false
});

// Instantiate a sentry client
var sentryClient = new raven.Client('https://f4ab035568994293b9a2a90727ccb5fc:a6dc87433f284952b2b5d629422ef7e6@sentry.io/103134');
sentryClient.patchGlobal();

/**
 * Gets a Datastore key from the kind/key pair in the request.
 *
 * @param {Object} requestData Cloud Function request data.
 * @param {string} requestData.Id Datastore ID string.
 * @returns {Object} Datastore key object.
 */
function getKeysFromRequestData(requestData, resouceType) {
    if (!requestData.Id) {
        throw new Error("Id not provided. Make sure you have a 'Id' property " +
            "in your request");
    }

    var ids = requestData.Id.split(',');
    var keys = [];

    for (var i = ids.length - 1; i >= 0; i--) {
        var listId = parseInt(ids[i], 10);
        var datastoreId = datastore.key([resouceType, listId]);
        keys.push(datastoreId);
    }

    return keys;
}

/**
 * Retrieves a record.
 *
 * @example
 * gcloud alpha functions call ds-get --data '{'kind':'gcf-test','key':'foobar'}'
 *
 * @param {Object} context Cloud Function context.
 * @param {Function} context.success Success callback.
 * @param {Function} context.failure Failure callback.
 * @param {Object} data Request data, in this case an object provided by the user.
 * @param {string} data.kind The Datastore kind of the data to retrieve, e.g. 'user'.
 * @param {string} data.key Key at which to retrieve the data, e.g. 5075192766267392.
 */
function getDatastore(data, resouceType) {
    var deferred = Q.defer();
    try {
        var keys = getKeysFromRequestData(data, resouceType);

        datastore.get(keys, function(err, entities) {
            if (err) {
                console.error(err);
                sentryClient.captureMessage(err);
                deferred.reject(new Error(err));
            }

            // The get operation will not fail for a non-existent entities, it just
            // returns null.
            if (!entities) {
                var error = 'Entity does not exist';
                console.error(error);
                sentryClient.captureMessage(error);
                deferred.reject(new Error(error));
            }

            deferred.resolve(entities);
        });

    } catch (err) {
        console.error(err);
        sentryClient.captureMessage(err);
        deferred.reject(new Error(err));
    }

    return deferred.promise;
}

/**
 * Format a list for ES sync
 *
 * @param {Object} listData List details from datastore.
 */
function formatESList(listId, listData) {
    listData['Id'] = listId;

    delete listData['FieldsMap.Value']
    delete listData['Contacts']
    delete listData['FieldsMap.Name']
    delete listData['FieldsMap.CustomField']
    delete listData['FieldsMap.Hidden']
    delete listData['fieldsmap.Value']
    delete listData['fieldsmap.Name']
    delete listData['fieldsmap.CustomField']
    delete listData['fieldsmap.Hidden']

    return listData;
}

/**
 * Add a list to ES
 *
 * @param {Object} listData List details from datastore.
 * Returns true if adding data works and false if not.
 */
function addToElastic(lists) {
    var deferred = Q.defer();
    var esActions = [];

    for (var i = lists.length - 1; i >= 0; i--) {
        var listId = lists[i].key.id;
        var listData = lists[i].data;
        var postListData = formatESList(listId, listData);

        var indexRecord = {
            index: {
                _index: 'lists',
                _type: 'list',
                _id: listId
            }
        };
        var dataRecord = postListData;
        esActions.push(indexRecord);
        esActions.push({
            data: dataRecord
        });
    }

    client.bulk({
        body: esActions
    }, function(error, response) {
        if (error) {
            sentryClient.captureMessage(error);
            deferred.reject(false);
        }
        deferred.resolve(true);
    });

    return deferred.promise;
}

/**
 * Syncs a list information to elasticsearch.
 *
 * @param {Object} Get list details from datastore.
 */
function getAndSyncElastic(lists) {
    var deferred = Q.defer();

    addToElastic(lists).then(function(status) {
        if (status) {
            deferred.resolve(true);
        } else {
            deferred.resolve(false);
        }
    });

    return deferred.promise;
}

function syncList(data) {
    var deferred = Q.defer();
    if (data.Method && data.Method.toLowerCase() === 'create') {
        getDatastore(data, 'MediaList').then(function(lists) {
            if (lists != null) {
                getAndSyncElastic(lists).then(function(elasticResponse) {
                    if (elasticResponse) {
                        deferred.resolve('Success!');
                    } else {
                        var error = 'Elastic sync failed';
                        sentryClient.captureMessage(error);
                        deferred.reject(new Error(error));
                        throw new Error(error);
                    }
                });
            } else {
                var error = 'List not found';
                sentryClient.captureMessage(error);
                deferred.reject(new Error(error));
                throw new Error(error);
            }
        }, function(error) {
            sentryClient.captureMessage(error);
            deferred.reject(new Error(error));
            throw new Error(error);
        });
    } else {
        // This case should never happen unless wrong pub/sub method is called.
        var error = 'Can not parse method ' + data.Method;
        sentryClient.captureMessage(error);
        deferred.reject(new Error(error));
        throw new Error(error);
    }

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

testSync({Id: '5097453078446080', Method: 'create'})