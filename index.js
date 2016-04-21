/**
 * @author Dmitriy Bizyaev
 */

'use strict';

/**
 *
 * @ignore
 */
const co = require('co');

/**
 *
 * @param {number} msecs
 * @returns {Promise}
 */
exports.wait = msecs => new Promise(resolve => void setTimeout(resolve, msecs));

/**
 *
 * @param {Promise} promise
 * @param {number} timeout
 * @returns {Promise}
 */
exports.withTimeout = (promise, timeout) => new Promise((resolve, reject) => {
    let settled = false;

    const cb = () => {
        settled = true;
        reject(new Error('Operation timed out'));
    };

    const to = setTimeout(cb, timeout);

    promise
        .then(res => {
            clearTimeout(to);
            if (!settled) resolve(res);
        })
        .catch(err => {
            clearTimeout(to);
            if (!settled) reject(err);
        });
});

/**
 * @typedef {Object} AsyncArrayProcessOptions
 * @property {number} threads
 */

/**
 *
 * @const {AsyncArrayProcessOptions}
 */
const defaultAsyncArrayProcessOptions = {
    threads: 1
};

/**
 *
 * @param {*[]} array
 * @param {function(item: *): Promise} fn
 * @param {AsyncArrayProcessOptions} options
 * @returns {Promise}
 */
exports.asyncMap = (array, fn, options) => co(function* () {
    options = Object.assign({}, defaultAsyncArrayProcessOptions, options);

    const itemsNum = array.length,
        threads = Math.min(itemsNum, options.threads),
        ret = new Array(itemsNum);

    let processed = 0,
        error = false;

    const doWork = () => co(function* () {
        while (processed < itemsNum) {
            if (error) break;

            const i = processed;
            processed++;

            ret[i] = yield fn(array[i]);
        }
    });

    const workPromises = [];

    for (let j = 0; j < threads; j++) workPromises.push(doWork());

    try {
        yield workPromises;
    }
    catch (err) {
        error = true;
        throw err;
    }

    return ret;
});

/**
 *
 * @param {*[]} array
 * @param {function(item: *): Promise} fn
 * @param {AsyncArrayProcessOptions} options
 * @returns {Promise}
 */
exports.asyncForEach = (array, fn, options) => co(function* () {
    options = Object.assign({}, defaultAsyncArrayProcessOptions, options);

    const itemsNum = array.length,
        threads = Math.min(itemsNum, options.threads);

    let processed = 0,
        error = false;

    const doWork = () => co(function* () {
        while (processed < itemsNum) {
            if (error) break;

            const i = processed;
            processed++;

            yield fn(array[i]);
        }
    });

    const workPromises = [];

    for (let j = 0; j < threads; j++) workPromises.push(doWork());

    try {
        yield workPromises;
    }
    catch (err) {
        error = true;
        throw err;
    }
});

/**
 * @class {Worker}
 */
exports.Worker = class {
    /**
     *
     * @param {function(item: *): Promise|*} processItemFn
     * @param {number} threads
     */
    constructor(processItemFn, threads) {
        this._processItemFn = processItemFn;
        this._queue = [];
        this._threads = threads || 1;
        this._threadsActive = 0;
    }

    /**
     *
     * @param {*} item
     */
    addItem(item) {
        this._queue.push(item);
        if (this._threadsActive < this._threads) this._doWork();
    }

    /**
     *
     * @private
     */
    _doWork() {
        const that = this;

        this._threadsActive++;

        co(function* () {
            let currentItem = null;

            while (currentItem = that._queue.shift())
                yield that._processItemFn(currentItem);

            that._threadsActive--;
        });
    }
};
