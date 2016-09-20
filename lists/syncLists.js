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

/**
 * Creates a easily searchable map of all contact Ids in a media list
 */
function mediaListToContactMap(mediaListId) {
    var deferred = Q.defer();

    // Get a particular media list by its Id then create a map out of it
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

/**
 * Format a contact for ES sync
 *
 * @param {Object} contactData Contact details from datastore.
 */
function formatESContact(contactId, contactData) {
    contactData['Id'] = contactId;

    if ('CustomFields.Name' in contactData && 'CustomFields.Value' in contactData) {
        // Populate a column for contactData
        contactData['CustomFields'] = [];
        for (var i = contactData['CustomFields.Name'].length - 1; i >= 0; i--) {
            var singleData = {};
            singleData.Name = contactData['CustomFields.Name'][i];
            singleData.Value = contactData['CustomFields.Value'][i];
            contactData['CustomFields'].push(singleData);
        }

        // Remove the name and value fields
        delete contactData['CustomFields.Name'];
        delete contactData['CustomFields.Value'];
    }

    return contactData;
}

function getContactsByIds(contactIds) {
    var deferred = Q.defer();
    try {
        var contactKeys = [];
        for (var i = contactIds.length - 1; i >= 0; i--) {
            var key = getKeyFromRequestData({
                Id: contactIds[i]
            }, 'Contact');
            contactKeys.push(key);
        }

        if (contactKeys.length !== 0) {
            datastore.get(contactKeys, function(err, entities) {
                if (err) {
                    console.error(err);
                    deferred.reject(new Error(error));
                }

                // The get operation will not fail for a non-existent entities, it just
                // returns null.
                if (!entities) {
                    var error = 'Entities do not exist';
                    console.error(error);
                    deferred.reject(new Error(error));
                }

                for (var i = entities.length - 1; i >= 0; i--) {
                    entities[i] = formatESContact(entities[i].key.id, entities[i].data);
                }

                deferred.resolve(entities);
            });
        } else {
            deferred.resolve([]);
        }
    } catch (err) {
        console.error(err);
        deferred.reject(new Error(error));
    }

    return deferred.promise;
}

/**
 * Gets all the contacts in ElasticSearch for a particular list
 * Returns a contact Id to ElasticId list and an array of duplicate mediaList ids
 */
function getElasticContactsByListId(listId) {
    var deferred = Q.defer();

    client.search({
        index: 'contacts',
        type: 'contact',
        size: 1000,
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
        var duplicateContactIds = {};

        // Create a map from API Contact Id to Elastic Id
        // Create a map of contact Id to how many are present in the ES data
        for (var i = hits.length - 1; i >= 0; i--) {
            contactIdToElasticId[hits[i]._source.data.Id] = hits[i]._id;

            if (hits[i]._source.data.Id in duplicateContactIds) {
                duplicateContactIds[hits[i]._source.data.Id] += 1;
            } else {
                duplicateContactIds[hits[i]._source.data.Id] = 1;
            }
        }

        // Remove duplicateIds that have 1 as value
        var duplicateKeys = Object.keys(duplicateContactIds);
        for (var i = duplicateKeys.length - 1; i >= 0; i--) {
            if (duplicateContactIds[duplicateKeys[i]] === 1) {
                delete duplicateContactIds[duplicateKeys[i]];
            }
        }

        deferred.resolve([contactIdToElasticId, Object.keys(duplicateContactIds)]);
    }, function(err) {
        console.trace(err.message);
        deferred.reject(new Error(err.message));
    });

    return deferred.promise;
}

/**
 * Syncs our API media list with the media list in ElasticSearch
 */
function syncList(data) {
    var deferred = Q.defer();

    // Get a map of media list from id to boolean (easily searchable)
    mediaListToContactMap(data).then(function(mediaListContactToMap) {
        // Get a API Id to ES Id map, and a duplicate list array
        getElasticContactsByListId(data.Id).then(function(contactLists) {
            var elasticContactList = contactLists[0];
            var duplicateContactIds = contactLists[1];

            var contactsToDelete = []; // This would be ES ids
            var contactsToAdd = []; // This would be API ids
            var mediaContactListKeys = Object.keys(mediaListContactToMap);
            var elasticContactListKeys = Object.keys(elasticContactList);

            // Loop through to find contacts that shouldn't be in ES
            for (var i = elasticContactListKeys.length - 1; i >= 0; i--) {
                if (!(elasticContactListKeys[i] in mediaListContactToMap)) {
                    contactsToDelete.push(elasticContactList[elasticContactListKeys[i]]);
                }
            }

            // Loop through to find duplicate contacts that shouldn't be in ES
            for (var i = duplicateContactIds.length - 1; i >= 0; i--) {
                contactsToDelete.push(elasticContactList[duplicateContactIds[i]]);
            }

            // Loop through to find contacts that should be in ES
            for (var i = mediaContactListKeys.length - 1; i >= 0; i--) {
                if (!(mediaContactListKeys[i] in elasticContactList)) {
                    contactsToAdd.push(mediaContactListKeys[i]);
                }
            }

            // Remove unnecessary contacts
            var esActions = [];
            for (var i = contactsToDelete.length - 1; i >= 0; i--) {
                var eachRecord = {
                    delete: {
                        _index: 'contacts',
                        _type: 'contact',
                        _id: contactsToDelete[i]
                    }
                };
                esActions.push(eachRecord);
            }

            // Add and format contacts that should be added
            getContactsByIds(contactsToAdd).then(function(contacts) {
                for (var i = contacts.length - 1; i >= 0; i--) {
                    var indexRecord = {
                        index: {
                            _index: 'contacts',
                            _type: 'contact'
                        }
                    };
                    var dataRecord = contacts[i];
                    esActions.push(indexRecord);
                    esActions.push({
                        data: dataRecord
                    });
                }
                // Remove contacts from ES that are not important anymore
                if (esActions.length > 0) {
                    client.bulk({
                        body: esActions
                    }, function(error, response) {
                        deferred.resolve(true);
                    });
                } else {
                    deferred.resolve(true);
                }
            });

        }, function(error) {
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

// testSync({Id: '5647943331741696'})