import { app, HttpRequest, HttpResponseInit, InvocationContext } from "@azure/functions";
import * as sql from "mssql";

// SQL connection configuration
const sqlConfig: sql.config = {
  server: "cockpit-sql-eus2.database.windows.net",
  database: "cockpit-db",
  authentication: {
    type: "azure-active-directory-default",
  },
  options: {
    encrypt: true,
    trustServerCertificate: false,
  },
};

let pool: sql.ConnectionPool | null = null;

async function getPool(): Promise<sql.ConnectionPool> {
  if (!pool) {
    pool = await sql.connect(sqlConfig);
  }
  return pool;
}

// GET /api/farms - List all farms for a tenant
async function getFarms(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const tenantId = request.query.get("tenantId") || "arrowfoot";

  try {
    const dbPool = await getPool();
    const result = await dbPool.request()
      .input("tenantId", sql.NVarChar, tenantId)
      .query(`
        SELECT
          f.FarmId as id,
          f.Name as name,
          f.Location as location,
          f.State as state,
          f.Latitude as lat,
          f.Longitude as lng,
          f.Integrator as integrator,
          (SELECT COUNT(*) FROM Houses h WHERE h.FarmId = f.FarmId) as houseCount,
          (SELECT SUM(CurrentBirds) FROM Houses h WHERE h.FarmId = f.FarmId) as totalBirds,
          (SELECT SUM(Capacity) FROM Houses h WHERE h.FarmId = f.FarmId) as totalCapacity
        FROM Farms f
        JOIN Tenants t ON f.TenantId = t.TenantId
        WHERE t.Name = @tenantId AND f.IsActive = 1
      `);

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.recordset),
    };
  } catch (error) {
    context.log(`Error fetching farms: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// GET /api/farms/:farmId - Get single farm with houses
async function getFarm(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const farmId = request.params.farmId;

  try {
    const dbPool = await getPool();

    // Get farm details
    const farmResult = await dbPool.request()
      .input("farmId", sql.UniqueIdentifier, farmId)
      .query(`
        SELECT
          FarmId as id, Name as name, Location as location, State as state,
          Latitude as lat, Longitude as lng, Integrator as integrator
        FROM Farms WHERE FarmId = @farmId
      `);

    if (farmResult.recordset.length === 0) {
      return { status: 404, body: "Farm not found" };
    }

    // Get houses with latest sensor data
    const housesResult = await dbPool.request()
      .input("farmId", sql.UniqueIdentifier, farmId)
      .query(`
        SELECT
          h.HouseId as id,
          h.Name as name,
          h.Capacity as capacity,
          h.CurrentBirds as currentBirds,
          h.BirdAgeInDays as birdAgeInDays,
          h.FlockId as flockId,
          h.Status as status,
          (
            SELECT TOP 1
              Temperature as temperature,
              Humidity as humidity,
              Ammonia as ammonia,
              CO2 as co2,
              FeedLevel as feedLevel,
              WaterFlow as waterFlow,
              Ventilation as ventilation,
              Lighting as lighting,
              Timestamp as timestamp
            FROM SensorReadings sr
            WHERE sr.HouseId = h.HouseId
            ORDER BY Timestamp DESC
            FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
          ) as latestSensors
        FROM Houses h
        WHERE h.FarmId = @farmId AND h.IsActive = 1
      `);

    const farm = {
      ...farmResult.recordset[0],
      houses: housesResult.recordset.map((h: any) => ({
        ...h,
        sensors: h.latestSensors ? JSON.parse(h.latestSensors) : null,
      })),
    };

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(farm),
    };
  } catch (error) {
    context.log(`Error fetching farm: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// GET /api/houses/:houseId/sensors - Get sensor history
async function getSensorHistory(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const houseId = request.params.houseId;
  const hours = parseInt(request.query.get("hours") || "24");

  try {
    const dbPool = await getPool();
    const result = await dbPool.request()
      .input("houseId", sql.NVarChar, houseId)
      .input("hours", sql.Int, hours)
      .query(`
        SELECT
          Temperature as temperature,
          Humidity as humidity,
          Ammonia as ammonia,
          CO2 as co2,
          FeedLevel as feedLevel,
          WaterFlow as waterFlow,
          Ventilation as ventilation,
          Lighting as lighting,
          Timestamp as timestamp
        FROM SensorReadings
        WHERE HouseId = @houseId
          AND Timestamp > DATEADD(HOUR, -@hours, GETUTCDATE())
        ORDER BY Timestamp ASC
      `);

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.recordset),
    };
  } catch (error) {
    context.log(`Error fetching sensor history: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// GET /api/alerts - Get active alerts
async function getAlerts(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const tenantId = request.query.get("tenantId") || "arrowfoot";
  const severity = request.query.get("severity");

  try {
    const dbPool = await getPool();
    let query = `
      SELECT
        a.AlertId as id,
        a.HouseId as houseId,
        h.Name as houseName,
        f.Name as farmName,
        a.Type as type,
        a.Severity as severity,
        a.Metric as metric,
        a.Value as value,
        a.Threshold as threshold,
        a.Message as message,
        a.CreatedAt as timestamp
      FROM Alerts a
      JOIN Houses h ON a.HouseId = h.HouseId
      JOIN Farms f ON h.FarmId = f.FarmId
      JOIN Tenants t ON f.TenantId = t.TenantId
      WHERE a.IsActive = 1 AND t.Name = @tenantId
    `;

    const req = dbPool.request().input("tenantId", sql.NVarChar, tenantId);

    if (severity) {
      query += " AND a.Severity = @severity";
      req.input("severity", sql.NVarChar, severity);
    }

    query += " ORDER BY a.CreatedAt DESC";

    const result = await req.query(query);

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.recordset),
    };
  } catch (error) {
    context.log(`Error fetching alerts: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// Register HTTP endpoints
app.http("getFarms", {
  methods: ["GET"],
  route: "farms",
  authLevel: "anonymous",
  handler: getFarms,
});

app.http("getFarm", {
  methods: ["GET"],
  route: "farms/{farmId}",
  authLevel: "anonymous",
  handler: getFarm,
});

app.http("getSensorHistory", {
  methods: ["GET"],
  route: "houses/{houseId}/sensors",
  authLevel: "anonymous",
  handler: getSensorHistory,
});

app.http("getAlerts", {
  methods: ["GET"],
  route: "alerts",
  authLevel: "anonymous",
  handler: getAlerts,
});

// ============================================
// Admin Endpoints
// ============================================

// GET /api/admin/tenants - List all tenants
async function getAdminTenants(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  try {
    const dbPool = await getPool();
    const result = await dbPool.request().execute("usp_Admin_GetTenants");

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.recordset),
    };
  } catch (error) {
    context.log(`Error fetching tenants: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// PUT /api/admin/tenants/:id - Update tenant
async function updateTenant(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const tenantId = request.params.id;

  try {
    const body = await request.json() as { name?: string; displayName?: string; color?: string };
    const dbPool = await getPool();

    await dbPool.request()
      .input("TenantId", sql.UniqueIdentifier, tenantId)
      .input("Name", sql.NVarChar, body.name || null)
      .input("DisplayName", sql.NVarChar, body.displayName || null)
      .input("Color", sql.NVarChar, body.color || null)
      .execute("usp_Admin_UpdateTenant");

    return { status: 200, body: "Updated" };
  } catch (error) {
    context.log(`Error updating tenant: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// GET /api/admin/farms - List all farms
async function getAdminFarms(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  try {
    const dbPool = await getPool();
    const result = await dbPool.request().execute("usp_Admin_GetFarms");

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.recordset),
    };
  } catch (error) {
    context.log(`Error fetching farms: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// PUT /api/admin/farms/:id - Update farm
async function updateFarm(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const farmId = request.params.id;

  try {
    const body = await request.json() as {
      name?: string;
      location?: string;
      state?: string;
      integrator?: string;
      tenantId?: string;
    };
    const dbPool = await getPool();

    await dbPool.request()
      .input("FarmId", sql.UniqueIdentifier, farmId)
      .input("Name", sql.NVarChar, body.name || null)
      .input("Location", sql.NVarChar, body.location || null)
      .input("State", sql.NVarChar, body.state || null)
      .input("Integrator", sql.NVarChar, body.integrator || null)
      .input("TenantId", sql.UniqueIdentifier, body.tenantId || null)
      .execute("usp_Admin_UpdateFarm");

    return { status: 200, body: "Updated" };
  } catch (error) {
    context.log(`Error updating farm: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// GET /api/admin/houses - List all houses
async function getAdminHouses(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  try {
    const dbPool = await getPool();
    const result = await dbPool.request().execute("usp_Admin_GetHouses");

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.recordset),
    };
  } catch (error) {
    context.log(`Error fetching houses: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// PUT /api/admin/houses/:id - Update house
async function updateHouse(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const houseId = request.params.id;

  try {
    const body = await request.json() as {
      name?: string;
      capacity?: number;
      currentBirds?: number;
      birdAgeInDays?: number;
      status?: string;
      farmId?: string;
    };
    const dbPool = await getPool();

    await dbPool.request()
      .input("HouseId", sql.UniqueIdentifier, houseId)
      .input("Name", sql.NVarChar, body.name || null)
      .input("Capacity", sql.Int, body.capacity || null)
      .input("CurrentBirds", sql.Int, body.currentBirds || null)
      .input("BirdAgeInDays", sql.Int, body.birdAgeInDays || null)
      .input("Status", sql.NVarChar, body.status || null)
      .input("FarmId", sql.UniqueIdentifier, body.farmId || null)
      .execute("usp_Admin_UpdateHouse");

    return { status: 200, body: "Updated" };
  } catch (error) {
    context.log(`Error updating house: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// Register Admin endpoints
app.http("getAdminTenants", {
  methods: ["GET"],
  route: "admin/tenants",
  authLevel: "anonymous",
  handler: getAdminTenants,
});

app.http("updateTenant", {
  methods: ["PUT"],
  route: "admin/tenants/{id}",
  authLevel: "anonymous",
  handler: updateTenant,
});

app.http("getAdminFarms", {
  methods: ["GET"],
  route: "admin/farms",
  authLevel: "anonymous",
  handler: getAdminFarms,
});

app.http("updateFarm", {
  methods: ["PUT"],
  route: "admin/farms/{id}",
  authLevel: "anonymous",
  handler: updateFarm,
});

app.http("getAdminHouses", {
  methods: ["GET"],
  route: "admin/houses",
  authLevel: "anonymous",
  handler: getAdminHouses,
});

app.http("updateHouse", {
  methods: ["PUT"],
  route: "admin/houses/{id}",
  authLevel: "anonymous",
  handler: updateHouse,
});

// ============================================
// Trends / Analytics Endpoints
// ============================================

// GET /api/trends/daily - Get daily trend data
async function getDailyTrends(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const tenantId = request.query.get("tenantId") || "arrowfoot";
  const days = parseInt(request.query.get("days") || "30");
  const farmId = request.query.get("farmId") || null;

  try {
    const dbPool = await getPool();
    const req = dbPool.request()
      .input("TenantId", sql.NVarChar, tenantId)
      .input("Days", sql.Int, days);

    if (farmId) {
      req.input("FarmId", sql.UniqueIdentifier, farmId);
    } else {
      req.input("FarmId", sql.UniqueIdentifier, null);
    }

    const result = await req.execute("usp_GetDailyTrends");

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.recordset),
    };
  } catch (error) {
    context.log(`Error fetching daily trends: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// GET /api/trends/summary - Get portfolio trend summary
async function getTrendSummary(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const tenantId = request.query.get("tenantId") || "arrowfoot";

  try {
    const dbPool = await getPool();

    // Get today vs 7 days ago comparison
    const result = await dbPool.request()
      .input("tenantId", sql.NVarChar, tenantId)
      .query(`
        WITH TodayData AS (
          SELECT
            AVG(d.AvgTemperature) as avgTemp,
            AVG(d.AvgHumidity) as avgHumidity,
            AVG(d.AvgAmmonia) as avgAmmonia,
            SUM(d.CriticalAlertCount) as criticalAlerts,
            SUM(d.WarningAlertCount) as warningAlerts,
            SUM(d.BirdCount) as totalBirds
          FROM DailyKPIs d
          JOIN Houses h ON d.HouseId = h.HouseId
          JOIN Farms f ON h.FarmId = f.FarmId
          JOIN Tenants t ON f.TenantId = t.TenantId
          WHERE t.Name = @tenantId
            AND d.Date = CAST(GETUTCDATE() AS DATE)
        ),
        LastWeekData AS (
          SELECT
            AVG(d.AvgTemperature) as avgTemp,
            AVG(d.AvgHumidity) as avgHumidity,
            AVG(d.AvgAmmonia) as avgAmmonia,
            SUM(d.CriticalAlertCount) as criticalAlerts,
            SUM(d.WarningAlertCount) as warningAlerts,
            SUM(d.BirdCount) as totalBirds
          FROM DailyKPIs d
          JOIN Houses h ON d.HouseId = h.HouseId
          JOIN Farms f ON h.FarmId = f.FarmId
          JOIN Tenants t ON f.TenantId = t.TenantId
          WHERE t.Name = @tenantId
            AND d.Date = CAST(DATEADD(DAY, -7, GETUTCDATE()) AS DATE)
        )
        SELECT
          t.avgTemp as todayAvgTemp,
          t.avgHumidity as todayAvgHumidity,
          t.avgAmmonia as todayAvgAmmonia,
          t.criticalAlerts as todayCriticalAlerts,
          t.warningAlerts as todayWarningAlerts,
          t.totalBirds as todayTotalBirds,
          l.avgTemp as lastWeekAvgTemp,
          l.avgHumidity as lastWeekAvgHumidity,
          l.avgAmmonia as lastWeekAvgAmmonia,
          l.criticalAlerts as lastWeekCriticalAlerts,
          l.warningAlerts as lastWeekWarningAlerts,
          l.totalBirds as lastWeekTotalBirds
        FROM TodayData t, LastWeekData l
      `);

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.recordset[0] || {}),
    };
  } catch (error) {
    context.log(`Error fetching trend summary: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

app.http("getDailyTrends", {
  methods: ["GET"],
  route: "trends/daily",
  authLevel: "anonymous",
  handler: getDailyTrends,
});

app.http("getTrendSummary", {
  methods: ["GET"],
  route: "trends/summary",
  authLevel: "anonymous",
  handler: getTrendSummary,
});
