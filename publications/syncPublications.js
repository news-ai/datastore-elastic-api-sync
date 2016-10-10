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
    log: 'trace',
    rejectUnauthorized: false
});

// Instantiate a sentry client
var sentryClient = new raven.Client('https://c2b3c727812f4643b73f40bee09e5108:fed6658dfeb94757b53cb062e81cdc68@sentry.io/103136');
sentryClient.patchGlobal();

/**
 * Gets a Datastore key from the kind/key pair in the request.
 *
 * @param {Object} requestData Cloud Function request data.
 * @param {string} requestData.Id Datastore ID string.
 * @returns {Object} Datastore key object.
 */
function getKeyFromRequestData(requestData) {
    if (!requestData.Id) {
        throw new Error('Id not provided. Make sure you have a "Id" property ' +
            'in your request');
    }

    var publicationId = parseInt(requestData.Id, 10);
    return datastore.key(['Publication', publicationId]);
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
function getDatastore(data) {
    var deferred = Q.defer();
    try {
        var key = getKeyFromRequestData(data);

        datastore.get(key, function(err, entity) {
            if (err) {
                console.error(err);
                sentryClient.captureMessage(err);
                deferred.reject(new Error(err));
            }

            // The get operation will not fail for a non-existent entity, it just
            // returns null.
            if (!entity) {
                var error = 'Entity does not exist';
                console.error(error);
                sentryClient.captureMessage(error);
                deferred.reject(new Error(error));
            }

            deferred.resolve(entity);
        });

    } catch (err) {
        console.error(err);
        sentryClient.captureMessage(err);
        deferred.reject(new Error(err));
    }

    return deferred.promise;
}

/**
 * Format a publication for ES sync
 *
 * @param {Object} publicationData Publication details from datastore.
 */
function formatESPublication(publicationId, publicationData) {
    publicationData['Id'] = publicationId;
    return publicationData;
}

/**
 * Add a publication to ES
 *
 * @param {Object} publicationData Publication details from datastore.
 * Returns true if adding data works and false if not.
 */
function addToElastic(publicationId, publicationData) {
    var deferred = Q.defer();

    var postPublicationData = formatESPublication(publicationId, publicationData);
    client.create({
        index: 'publications',
        type: 'publication',
        _id: publicationId,
        body: {
            data: postPublicationData
        }
    }, function(error, response) {
        if (error) {
            console.error(error);
            sentryClient.captureMessage(error);
            deferred.resolve(false);
        } else {
            deferred.resolve(true);
        }
    });

    return deferred.promise;
}

/**
 * Syncs a publication information to elasticsearch.
 *
 * @param {Object} publication Publication details from datastore.
 */
function getAndSyncElastic(publication) {
    var deferred = Q.defer();

    var publicationData = publication.data;
    var publicationId = publication.key.id;

    addToElastic(publicationId, publicationData).then(function(status) {
        if (status) {
            deferred.resolve(true);
        } else {
            deferred.resolve(false);
        }
    });

    return deferred.promise;
}

function syncPublication(data) {
    var deferred = Q.defer();

    getDatastore(data).then(function(publication) {
        if (publication != null) {
            getAndSyncElastic(publication).then(function(elasticResponse) {
                if (elasticResponse) {
                    deferred.resolve(true);
                } else {
                    var error = 'Elastic sync failed';
                    sentryClient.captureMessage(error);
                    deferred.reject(new Error(error));
                    throw new Error(error);
                }
            });
        } else {
            var error = 'Elastic sync failed';
            sentryClient.captureMessage(error);
            deferred.reject(new Error(error));
            throw new Error(error);
        }
    }, function(error) {
        sentryClient.captureMessage(error);
        deferred.reject(new Error(error));
        throw new Error(error);
    });

    return deferred.promise;
};

/**
 * Triggered from a message on a Pub/Sub topic.
 *
 * @param {Object} context Cloud Function context.
 * @param {Function} context.success Success callback.
 * @param {Function} context.failure Failure callback.
 * @param {Object} data Request data, in this case an object provided by the Pub/Sub trigger.
 * @param {Object} data.message Message that was published via Pub/Sub.
 */
exports.syncPublications = function syncPublications(data) {
    return syncPublication(data);
};