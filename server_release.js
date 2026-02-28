const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const bodyParser = require('body-parser');

const app = express();
const port = 8011;

app.use(bodyParser.json({ limit: '10000mb' }));
app.use(bodyParser.urlencoded({ limit: '10000mb', extended: true }));
app.use(cors());

// 数据库配置常量
const DB_CONFIG = {
    host: 'localhost',
    user: 'root',
    password: 'Guoyanjun123.'
};

let dbName = `terminal_${Date.now()}`;

// Initialize database and create Visit and Terminal tables
async function initDatabase() {
    // // console.log(`Initializing database: ${dbName}...`);
    
    // 创建一个新的基础连接，避免使用全局关闭的连接
    const baseDb = mysql.createConnection({
        host: DB_CONFIG.host,
        user: DB_CONFIG.user,
        password: DB_CONFIG.password
    });

    try {
        await new Promise((resolve, reject) => {
            baseDb.connect(err => err ? reject(`Base connection failed: ${err.message}`) : resolve());
        });

        await new Promise((resolve, reject) => {
            baseDb.query(`CREATE DATABASE IF NOT EXISTS \`${dbName}\``, err =>
                err ? reject(`Failed to create database: ${err.message}`) : resolve()
            );
        });

        // 任务完成后关闭基础连接
        baseDb.end();

        const db = mysql.createConnection({
            ...DB_CONFIG,
            database: dbName
        });

        await new Promise((resolve, reject) => {
            db.connect(err => err ? reject(`Failed to connect to new database: ${err.message}`) : resolve());
        });

        // Create Visit table with proper indexes
        const createVisitTable = `
            CREATE TABLE IF NOT EXISTS Visit (
                拜访记录编号 VARCHAR(50),
                拜访开始时间 VARCHAR(50),
                拜访结束时间 VARCHAR(50),
                拜访人 VARCHAR(50),
                客户名称 VARCHAR(100),
                客户编码 VARCHAR(50),
                拜访用时 INT,
                INDEX idx_visit_customer (客户编码),
                INDEX idx_visit_time (拜访用时),
                INDEX idx_visit_start (拜访开始时间)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        `;

        // Create Terminal table (unique customer code to avoid JOIN bloat)
        const createTerminalTable = `
            CREATE TABLE IF NOT EXISTS Terminal (
                客户编码 VARCHAR(50),
                所属片区 VARCHAR(100),
                所属大区 VARCHAR(100),
                UNIQUE INDEX idx_terminal_customer (客户编码)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        `;

        // Execute table creation queries
        await new Promise((resolve, reject) => {
            db.query(createVisitTable, err => err ? reject(`Failed to create Visit table: ${err.message}`) : resolve());
        });

        await new Promise((resolve, reject) => {
            db.query(createTerminalTable, err => err ? reject(`Failed to create Terminal table: ${err.message}`) : resolve());
        });

        // Close old DB connection if exists (prevent leaks during re-init)
        const oldDb = app.get('db');
        if (oldDb) {
            try { oldDb.end(); } catch (e) {}
        }

        // Store database connection in app instance for later use
        app.set('db', db);
        // // console.log(`Database initialization completed: ${dbName}`);
        // // console.log('Visit and Terminal tables have been created successfully');

    } catch (error) {
        console.error('Database initialization failed:', error);
        // If initial startup fails, exit. If re-init fails, just log.
        if (process.uptime() < 10) {
            process.exit(1);
        }
    }
}

// Upload Visit records
app.post('/api/audit_visit/search_time/uploadVisit', (req, res) => {
    const db = app.get('db');
    const records = req.body.records;

    
    // Validate input data
    if (!Array.isArray(records) || records.length === 0) {
        return res.status(400).json({ success: false, error: 'Invalid Visit data provided' });
    }

    // Format records for bulk insertion
    const values = records.map(r => [
        r.拜访记录编号 || null,
        r.拜访开始时间 || null,
        r.拜访结束时间 || null,
        r.拜访人 || null,
        r.客户名称 || null,
        r.客户编码 || null,
        typeof r.拜访用时 === 'string' ? parseInt(r.拜访用时) || 0 : (r.拜访用时 || 0)
    ]);

    // Bulk insert Visit records
    const sql = 'INSERT INTO Visit (拜访记录编号, 拜访开始时间, 拜访结束时间, 拜访人, 客户名称, 客户编码, 拜访用时) VALUES ?';
    db.query(sql, [values], (err, result) => {
        if (err) {
            console.error('Failed to insert Visit records:', err);
            res.status(500).json({ success: false, error: err.message });
        } else {
            res.json({ success: true, message: `${result.affectedRows} Visit records imported successfully` });
        }
    });
});

// Upload Terminal records (use INSERT IGNORE to avoid duplicate customer code errors)
app.post('/api/audit_visit/search_time/uploadTerminal', (req, res) => {
    const db = app.get('db');
    const records = req.body.records;
    
    // Validate input data
    if (!Array.isArray(records) || records.length === 0) {
        return res.status(400).json({ success: false, error: 'Invalid Terminal data provided' });
    }

    // Format records for bulk insertion
    const values = records.map(r => [
        r.客户编码 || null,
        r.所属片区 || null,
        r.所属大区 || null
    ]);

    // Bulk insert Terminal records with duplicate handling
    const sql = 'INSERT IGNORE INTO Terminal (客户编码, 所属片区, 所属大区) VALUES ?';
    db.query(sql, [values], (err, result) => {
        if (err) {
            console.error('Failed to insert Terminal records:', err);
            res.status(500).json({ success: false, error: err.message });
        } else {
            res.json({ success: true, message: `${result.affectedRows} Terminal records imported successfully (duplicates automatically skipped)` });
        }
    });
});

// Query merged data (LEFT JOIN Visit + Terminal) -- use DISTINCT to remove duplicates
app.get('/api/audit_visit/search_time/getMinutes', (req, res) => {
    const db = app.get('db');
    
    // Parse and validate query parameters
    let {
        maxMinutes = 5,
        visitor = '',
        customerName = '',
        customerCode = '',
        startDate = '',
        endDate = '',
        area = '',
        region = ''
    } = req.query;

    maxMinutes = parseInt(maxMinutes) || 5;

    // Build dynamic WHERE clause for filtering
    let conditions = ['v.`拜访用时` <= ?'];
    let params = [maxMinutes];

    if (visitor) {
        conditions.push('v.`拜访人` LIKE ?');
        params.push(`%${visitor}%`);
    }
    if (customerName) {
        conditions.push('v.`客户名称` LIKE ?');
        params.push(`%${customerName}%`);
    }
    if (customerCode) {
        conditions.push('v.`客户编码` LIKE ?');
        params.push(`%${customerCode}%`);
    }
    if (startDate) {
        conditions.push('v.`拜访开始时间` >= ?');
        params.push(startDate);
    }
    if (endDate) {
        conditions.push('v.`拜访开始时间` <= ?');
        params.push(endDate + ' 23:59:59');
    }
    // Filter by area
    if (area) {
        conditions.push('t.`所属片区` LIKE ?');
        params.push(`%${area}%`);
    }
    // Filter by region
    if (region) {
        conditions.push('t.`所属大区` LIKE ?');
        params.push(`%${region}%`);
    }

    const whereClause = conditions.join(' AND ');

    // Construct SQL query with LEFT JOIN and DISTINCT to eliminate duplicate rows
    const sql = `
        SELECT DISTINCT
            v.拜访记录编号,
            v.拜访开始时间,
            v.拜访结束时间,
            v.拜访人,
            v.客户名称,
            v.客户编码,
            v.拜访用时,
            t.所属片区,
            t.所属大区
        FROM Visit v
        LEFT JOIN Terminal t ON v.客户编码 = t.客户编码
        WHERE ${whereClause}
        ORDER BY v.\`拜访用时\` ASC, v.\`拜访开始时间\` DESC;
    `;

    // Execute query and return results
    db.query(sql, params, (err, results) => {
        if (err) {
            console.error('Failed to query merged data:', err);
            res.status(500).json({ success: false, error: err.message });
        } else {
            res.json({ success: true, data: results });
        }
    });
});

// Get all area list (for frontend dropdown selection)
app.get('/api/audit_visit/search_time/getAreas', (req, res) => {
    const db = app.get('db');
    const sql = 'SELECT DISTINCT 所属片区 FROM Terminal WHERE 所属片区 IS NOT NULL AND 所属片区 != "" ORDER BY 所属片区';
    db.query(sql, (err, results) => {
        if (err) {
            res.status(500).json({ success: false, error: err.message });
        } else {
            res.json({ success: true, data: results.map(r => r.所属片区) });
        }
    });
});

// Get all region list (for frontend dropdown selection)
app.get('/api/audit_visit/search_time/getRegions', (req, res) => {
    const db = app.get('db');
    const sql = 'SELECT DISTINCT 所属大区 FROM Terminal WHERE 所属大区 IS NOT NULL AND 所属大区 != "" ORDER BY 所属大区';
    db.query(sql, (err, results) => {
        if (err) {
            res.status(500).json({ success: false, error: err.message });
        } else {
            res.json({ success: true, data: results.map(r => r.所属大区) });
        }
    });
});

// ============ Manual Cleanup Logic (Replaces Heartbeat) ============

app.post('/api/audit_visit/search_time/cleanup', async (req, res) => {
    try {
        // 1. Drop existing DB
        await dropDatabase();
        
        // 2. Generate new name and Re-initialize
        dbName = `terminal_${Date.now()}`;
        await initDatabase();
        
        res.json({ success: true, message: `Database has been reset. New DB: ${dbName}` });
    } catch (error) {
        console.error('Failed to cleanup database:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Core function to drop database
async function dropDatabase() {
    // Close current database connection if exists
    const db = app.get('db');
    if (db) {
        try { 
            await new Promise((resolve) => db.end(resolve)); 
            // // console.log('Closed existing database connection');
        } catch (e) { 
            console.error('Error closing database connection:', e.message);
        }
    }

    // Create new connection for cleanup operation
    const cleanupDb = mysql.createConnection({
        host: DB_CONFIG.host,
        user: DB_CONFIG.user,
        password: DB_CONFIG.password
    });

    await new Promise((resolve, reject) => {
        cleanupDb.connect(err => err ? reject(err) : resolve());
    });

    // Execute database drop command
    await new Promise((resolve, reject) => {
        cleanupDb.query(`DROP DATABASE IF EXISTS \`${dbName}\``, err => {
            if (err) reject(err);
            else resolve();
        });
    });

    // Close cleanup connection
    cleanupDb.end();
    // // console.log(`Dropped database: ${dbName}`);
}

// Setup process cleanup on exit (Ctrl+C or kill signal)
function setupProcessCleanup() {
    async function handleExit(signal) {
        // console.log(`\nReceived ${signal} signal, cleaning up database before exit...`);
        try {
            await dropDatabase();
            // console.log('Cleanup completed, process exiting');
        } catch (error) {
            console.error('Error occurred during cleanup:', error.message);
        }
        process.exit(0);
    }

    // Register exit handlers for common termination signals
    process.on('SIGINT', () => handleExit('SIGINT'));
    process.on('SIGTERM', () => handleExit('SIGTERM'));
}

// Initialize database and start server
initDatabase().then(() => {
    setupProcessCleanup();
    
    app.listen(port, () => {
        // console.log('='.repeat(60));
        console.log(`Server running on http://localhost:${port}`);
        // console.log(`Current database: ${dbName}`);
        // console.log('Server is running continuously. Use the "Delete Database" button to reset data.');
        // console.log('='.repeat(60));
    });
}).catch(error => {
    console.error('Failed to start server:', error);
    process.exit(1);
});