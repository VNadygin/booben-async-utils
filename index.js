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

const isNat = x => typeof x === 'number' && !isNaN(x) && x % 1 === 0 && x > 0;

/**
 * @typedef {Object} WorkerOptions
 * @property {number} queueLength
 */

/**
 * @class {Worker}
 */
exports.Worker = class {
    /**
     *
     * @param {function(item: *): Promise|*} processItemFn
     * @param {number} threads
     * @param {WorkerOptions} options
     */
    constructor(processItemFn, threads, options) {
        if (typeof processItemFn !== 'function')
            throw new Error('processItemFn must be a function');
        
        if (!isNat(threads))
            throw new Error('threads must be a positive integer');
        
        options = options || {};

        this._processItemFn = processItemFn;
        this._threads = threads;
        this._queueLength = options.queueLength || 0;
        this._queue = [];
        this._threadsActive = 0;
        this._itemCallbacks = new Map();
    }

    /**
     *
     * @param {*} item
     */
    addItem(item) {
        if (this._queue.length >= this._queueLength) return false;
        
        this._queue.push(item);
        if (this._threadsActive < this._threads) this._doWork();
        return true;
    }

    /**
     * 
     * @param {*} item
     * @returns {Promise}
     */
    processItem(item) {
        if (!this.addItem(item))
            return Promise.reject(new Error('Queue is full'));
        
        return new Promise((resolve, reject) =>
            void this._itemCallbacks.set(item, { resolve, reject }));
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

            while (currentItem = that._queue.shift()) {
                const callbacks = that._itemCallbacks.get(currentItem);
                that._itemCallbacks.delete(currentItem);
                
                try {
                    const res = yield Promise.resolve(that._processItemFn(currentItem));
                    if (callbacks) callbacks.resolve(res);
                }
                catch (err) {
                    if (callbacks) callbacks.reject(err);
                }
            }

            that._threadsActive--;
        });
    }
};
