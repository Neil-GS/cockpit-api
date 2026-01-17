-- ============================================
-- Grant Function App Managed Identity Access
-- Run this in SSMS as admin-cockpit@godfreylabs.com
-- ============================================

-- Create contained user for the Function App managed identity
CREATE USER [cockpit-api-eus2] FROM EXTERNAL PROVIDER;
GO

-- Grant read/write access to all tables
ALTER ROLE db_datareader ADD MEMBER [cockpit-api-eus2];
GO

ALTER ROLE db_datawriter ADD MEMBER [cockpit-api-eus2];
GO

-- Grant execute on stored procedures
GRANT EXECUTE TO [cockpit-api-eus2];
GO

-- Verify
SELECT name, type_desc FROM sys.database_principals WHERE name = 'cockpit-api-eus2';
GO

PRINT 'Function App managed identity granted access for API operations';
