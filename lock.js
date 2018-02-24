'use strict';

const prefix = '/tmp/rdb-init-lock-',
	timeout = 200,
	fs = require('fs');

const fileLock = {
	set: key => {
		try {
			return !fs.existsSync(prefix+key)
				&& fs.writeFileSync(prefix+key, key) === undefined;
		}
		catch(error) {
			console.error('cannot set lock for', key, error);
			return false;
		}
	},
	unset: key => {
		try {
			return !fs.existsSync(prefix+key)
				|| fs.unlinkSync(prefix+key) === undefined;
		}
		catch(error) {
			console.error('cannot unset lock of', key, error);
			return false;
		}
	}
};

module.exports = {
	set: key => new Promise(resolve => {
		let i = setInterval(() => fileLock.set(key) ? resolve(clearInterval(i) || key) : false, timeout);
	}),
	unset: key => new Promise((resolve, reject) => fileLock.unset(key) ? resolve(key) : reject(key))
};
