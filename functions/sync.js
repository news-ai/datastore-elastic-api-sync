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
    log: 'trace',
    rejectUnauthorized: false
});

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

    return datastore.key(['Contact', requestData.Id]);
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

/**
 * Format a contact for ES sync
 *
 * @param {Object} contactData Contact details from datastore.
 */
function formatESContact(contactId, contactData) {
    contactData['Id'] = contactId;

    if ('CustomFields.Name' in contactData) {
        delete contactData["CustomFields.Name"];
    }

    if ('CustomFields.Value' in contactData) {
        delete contactData["CustomFields.Value"];
    }

    return contactData;
}

/**
 * Format a contact for ES sync
 *
 * @param {Object} contactData Contact details from datastore.
 * Returns true if adding data works and false if not.
 */
function addToElastic(contactId, contactData) {
    var deferred = Q.defer();

    var postContactData = formatESContact(contactId, contactData);
    console.log(postContactData);
    client.create({
        index: 'contacts',
        type: 'contact',
        body: {
            data: postContactData
        }
    }, function(error, response) {
        if (error) {
            console.error(error);
            deferred.resolve(false);
        } else {
            deferred.resolve(true);
        }
    });

    return deferred.promise;
}

/**
 * Syncs a contact information to elasticsearch.
 *
 * @param {Object} contact Contact details from datastore.
 */
function getAndSyncElastic(contact) {
    var deferred = Q.defer();

    var contactData = contact.data;
    var contactId = contact.key.id;

    client.search({
        index: 'contacts',
        type: 'contact',
        body: {
            query: {
                match: {
                    'data.Id': contactId
                }
            }
        }
    }).then(function(resp) {
        var hits = resp.hits.hits;

        // Delete the current hit
        if (hits.length > 0) {
            var esContact = hits[0];
            client.delete({
                index: 'contacts',
                type: 'contact',
                id: esContact._id
            }, function(error, response) {
                // Add a new index
                addToElastic(contactId, contactData).then(function(status) {
                    if (status) {
                        deferred.resolve(true);
                    } else {
                        deferred.resolve(false);
                    }
                });
            });
        } else {
            addToElastic(contactId, contactData).then(function(status) {
                if (status) {
                    deferred.resolve(true);
                } else {
                    deferred.resolve(false);
                }
            });
        }
    }, function(err) {
        console.trace(err.message);
        deferred.reject(new Error(error));
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
exports.sync = function sync (context, data) {
    getDatastore(data).then(function(contact) {
        if (contact != null) {
            getAndSyncElastic(contact).then(function(elasticResponse) {
                if (elasticResponse) {
                    context.success();
                } else {
                    context.failure('Elastic sync failed');
                }
            });
        } else {
            context.failure('Contact not found');
        }
    }, function(error) {
        console.error(error);
        context.failure(err);
    });
};

// var testSync = function testSync (data) {
//     getDatastore(data).then(function(contact) {
//         if (contact != null) {
//             getAndSyncElastic(contact).then(function(elasticResponse) {
//                 console.log(elasticResponse);
//                 if (elasticResponse) {
//                     console.log('Success');
//                 } else {
//                     console.error('Elastic sync failed');
//                 }
//             });
//         } else {
//             console.error('Elastic sync failed');
//         }
//     }, function(error) {
//         console.error(error);
//     });
// };

// testSync({Id: 6332877872562176})