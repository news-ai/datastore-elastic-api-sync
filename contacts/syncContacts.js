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

    var contactId = parseInt(requestData.Id, 10);
    return datastore.key([resouceType, contactId]);
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

    var data = {
        Id: mediaListId
    };

    getDatastore(data, 'MediaList').then(function(mediaList) {
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
 * Add a contact to ES
 *
 * @param {Object} contactData Contact details from datastore.
 * Returns true if adding data works and false if not.
 */
function addToElastic(contactId, contactData) {
    var deferred = Q.defer();

    var postContactData = formatESContact(contactId, contactData);
    mediaListToContactMap(contactData['ListId']).then(function(mediaListContactToHashMap) {
        if (contactId in mediaListContactToHashMap) {
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
        size: 10000,
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
            var esActions = [];
            for (var i = hits.length - 1; i >= 0; i--) {
                var eachRecord = {
                    delete: {
                        _index: 'contacts',
                        _type: 'contact',
                        _id: hits[i]._id
                    }
                };
                esActions.push(eachRecord);
            }

            client.bulk({
                body: esActions
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
        deferred.reject(new Error(err.message));
    });

    return deferred.promise;
}

function syncContact(data) {
    var deferred = Q.defer();
    getDatastore(data, 'Contact').then(function(contact) {
        if (contact != null) {
            getAndSyncElastic(contact).then(function(elasticResponse) {
                if (elasticResponse) {
                    deferred.resolve('Success!');
                } else {
                    var error = "Elastic sync failed";
                    deferred.reject(new Error(error));
                    throw new Error(error);
                }
            });
        } else {
            var error = "Contact not found";
            deferred.reject(new Error(error));
            throw new Error(error);
        }
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
exports.syncContacts = function syncContacts(data) {
    return syncContact(data);
};

function testSync(data) {
    return syncContact(data);
};

// testSync({Id: '6095325244686336'})