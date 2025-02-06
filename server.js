const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const xlsx = require('xlsx');
const fs = require('fs');
const cors = require('cors');
const sql = require('mssql/msnodesqlv8');

const app = express();
app.use(cors());
app.use(express.json());

// Configure file upload
const upload = multer({ dest: 'uploads/' });

const config = {
    driver: 'msnodesqlv8',
    connectionString: 'Driver={SQL Server};Server=localhost;Database=StudentPortalDb;Trusted_Connection=yes;'
};
async function testConnection() {
    try {
        let pool = await sql.connect(config);
        console.log('Connected to SQL Server');
        
        // Test query
        const result = await pool.request().query('SELECT @@VERSION as version');
        console.log('SQL Server version:', result.recordset[0].version);
    } catch (err) {
        console.error('SQL Connection Error:', err);
        console.error('Error details:', {
            code: err.code,
            number: err.number,
            state: err.state,
            message: err.message
        });
    }
}
testConnection();
// Parse CSV file
function parseCSV(filePath) {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => resolve(results))
            .on('error', reject);
    });
}

// Parse Excel file
function parseExcel(filePath) {
    const workbook = xlsx.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    return xlsx.utils.sheet_to_json(workbook.Sheets[sheetName]);
}

// Migrate data to SQL Server
async function migrateToSQL(data, tableName, mappings) {
    let pool;
    try {
        pool = await sql.connect(config);
        
        // Debug logs
        console.log('Data received:', data);
        console.log('Mappings received:', mappings);
        
        for (let rowIndex = 0; rowIndex < data.length; rowIndex++) {
            const row = data[rowIndex];
            const columns = Object.keys(mappings);
            
            // Debug log for each row
            console.log('Processing row:', row);
            console.log('Available columns:', columns);
            
            // Get values using the correct mapping
            const values = columns.map(sqlCol => {
                const fileCol = mappings[sqlCol];
                const value = row[fileCol];
                console.log(`Mapping ${fileCol} -> ${sqlCol}, Value:`, value);
                return value;
            });
            
            const paramNames = columns.map((_, index) => `@p${rowIndex}_${index}`);
            
            const query = `
                INSERT INTO ${tableName} (${columns.join(', ')})
                VALUES (${paramNames.join(', ')})
            `;

            console.log('Executing query:', query);

            const request = pool.request();
            
            columns.forEach((col, index) => {
                request.input(`p${rowIndex}_${index}`, values[index]);
            });

            await request.query(query);
        }
        
        return true;
    } catch (error) {
        console.error('Migration Error:', error);
        console.error('Error details:', {
            data: data,
            mappings: mappings,
            tableName: tableName
        });
        throw error;
    } finally {
        if (pool) {
            await pool.close();
        }
    }
}

// Handle file upload and migration
app.post('/api/migrate', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            throw new Error('No file uploaded');
        }

        const { tableName } = req.body;
        const mappings = JSON.parse(req.body.mappings);
        const fileType = req.file.originalname.split('.').pop().toLowerCase();
        let data = [];

        // Parse file based on type
        if (fileType === 'csv') {
            data = await parseCSV(req.file.path);
        } else if (['xlsx', 'xls'].includes(fileType)) {
            data = parseExcel(req.file.path);
        } else {
            throw new Error('Unsupported file type');
        }

        // Migrate data
        await migrateToSQL(data, tableName, mappings);

        // Clean up uploaded file
        fs.unlinkSync(req.file.path);

        res.json({ 
            success: true, 
            message: `Migrated ${data.length} records successfully` 
        });

    } catch (error) {
        console.error('API Error:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});
// Add this new endpoint to get all table names
app.get('/api/tables', async (req, res) => {
    try {
        const pool = await sql.connect(config);
        const result = await pool.request().query(`
            SELECT TABLE_NAME 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_TYPE = 'BASE TABLE'
        `);
        
        res.json({ 
            success: true, 
            tables: result.recordset.map(row => row.TABLE_NAME) 
        });
    } catch (error) {
        console.error('Error fetching tables:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});
// Add this new endpoint to get column names for a specific table
app.get('/api/columns/:tableName', async (req, res) => {
    try {
        const pool = await sql.connect(config);
        const result = await pool.request()
            .input('tableName', sql.VarChar, req.params.tableName)
            .query(`
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = @tableName
                ORDER BY ORDINAL_POSITION
            `);
        
        res.json({ 
            success: true, 
            columns: result.recordset.map(row => ({
                name: row.COLUMN_NAME,
                type: row.DATA_TYPE,
                maxLength: row.CHARACTER_MAXIMUM_LENGTH
            }))
        });
    } catch (error) {
        console.error('Error fetching columns:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});