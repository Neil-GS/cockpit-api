import { app, HttpRequest, HttpResponseInit, InvocationContext } from "@azure/functions";
import * as sql from "mssql";

// Import IoT Hub trigger to ensure it's registered
import "./ingestSensorData";

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

// GET /api/farms - List all farms (optionally filtered by tenant)
async function getFarms(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const tenantId = request.query.get("tenantId") || null;

  try {
    const dbPool = await getPool();

    // Get all farms (optionally filtered by tenant)
    const farmsReq = dbPool.request();
    let farmsQuery = `
      SELECT
        f.FarmId as id,
        f.TenantId as tenantId,
        f.Name as name,
        f.Location as location,
        f.State as state,
        f.Latitude as lat,
        f.Longitude as lng,
        f.Integrator as integrator
      FROM Farms f
      JOIN Tenants t ON f.TenantId = t.TenantId
      WHERE f.IsActive = 1
    `;
    if (tenantId) {
      farmsQuery += ` AND t.Name = @tenantId`;
      farmsReq.input("tenantId", sql.NVarChar, tenantId);
    }
    const farmsResult = await farmsReq.query(farmsQuery);

    // Get all houses (optionally filtered by tenant)
    const housesReq = dbPool.request();
    let housesQuery = `
      SELECT
        h.HouseId as id,
        h.FarmId as farmId,
        h.Name as name,
        h.Capacity as capacity,
        h.CurrentBirds as currentBirds,
        h.BirdAgeInDays as birdAgeInDays,
        h.BirdAgeInDays as ageInDays,
        h.FlockId as flockId,
        h.Status as status,
        h.DeviceId as deviceId,
        h.UpdatedAt as lastUpdated
      FROM Houses h
      JOIN Farms f ON h.FarmId = f.FarmId
      JOIN Tenants t ON f.TenantId = t.TenantId
      WHERE h.IsActive = 1
    `;
    if (tenantId) {
      housesQuery += ` AND t.Name = @tenantId`;
      housesReq.input("tenantId", sql.NVarChar, tenantId);
    }
    housesQuery += ` ORDER BY h.FarmId, h.Name`;
    const housesResult = await housesReq.query(housesQuery);

    // Get latest sensor data for each house
    const sensorsReq = dbPool.request();
    let sensorsQuery = `
      WITH LatestEvents AS (
        SELECT
          se.HouseId,
          et.Code,
          se.Value,
          se.BoolValue,
          ROW_NUMBER() OVER (PARTITION BY se.HouseId, et.Code ORDER BY se.Timestamp DESC) as rn
        FROM SensorEvents se
        JOIN SensorEventTypes et ON se.EventTypeId = et.EventTypeId
        JOIN Houses h ON se.HouseId = h.HouseId
        JOIN Farms f ON h.FarmId = f.FarmId
        JOIN Tenants t ON f.TenantId = t.TenantId
        WHERE se.Timestamp >= DATEADD(HOUR, -24, GETUTCDATE())
    `;
    if (tenantId) {
      sensorsQuery += ` AND t.Name = @tenantId`;
      sensorsReq.input("tenantId", sql.NVarChar, tenantId);
    }
    sensorsQuery += `
      )
      SELECT HouseId, Code, Value, BoolValue
      FROM LatestEvents
      WHERE rn = 1
    `;
    const sensorsResult = await sensorsReq.query(sensorsQuery);

    // Build sensor data map by house
    const sensorsByHouse: Record<string, Record<string, number>> = {};
    for (const sensor of sensorsResult.recordset) {
      if (!sensorsByHouse[sensor.HouseId]) {
        sensorsByHouse[sensor.HouseId] = {
          temperature: 75,
          humidity: 55,
          ammonia: 15,
          co2: 2000,
          feedLevel: 70,
          waterFlow: 5,
          ventilation: 15000,
          lighting: 20,
        };
      }
      // Map event codes to sensor fields
      const codeMap: Record<string, string> = {
        temperature: "temperature",
        humidity: "humidity",
        ammonia: "ammonia",
        co2: "co2",
        feed_level: "feedLevel",
        water_flow: "waterFlow",
        fan_speed: "ventilation",
        lighting: "lighting",
      };
      if (codeMap[sensor.Code]) {
        sensorsByHouse[sensor.HouseId][codeMap[sensor.Code]] = sensor.Value ?? 0;
      }
    }

    // Map houses to farms
    const housesByFarm: Record<string, unknown[]> = {};
    for (const house of housesResult.recordset) {
      if (!housesByFarm[house.farmId]) {
        housesByFarm[house.farmId] = [];
      }
      housesByFarm[house.farmId].push({
        ...house,
        sensors: sensorsByHouse[house.id] || {
          temperature: 75,
          humidity: 55,
          ammonia: 15,
          co2: 2000,
          feedLevel: 70,
          waterFlow: 5,
          ventilation: 15000,
          lighting: 20,
          timestamp: new Date(),
        },
        alerts: [], // Alerts fetched separately
      });
    }

    // Build final farms array
    const farms = farmsResult.recordset.map((farm: any) => {
      const houses = housesByFarm[farm.id] || [];
      return {
        ...farm,
        coordinates: { lat: farm.lat, lng: farm.lng },
        houses,
        totalCapacity: houses.reduce((sum: number, h: any) => sum + (h.capacity || 0), 0),
        totalBirds: houses.reduce((sum: number, h: any) => sum + (h.currentBirds || 0), 0),
        createdAt: new Date(),
        updatedAt: new Date(),
      };
    });

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(farms),
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

// GET /tenants - List all visible tenants (public)
async function getTenants(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  try {
    const dbPool = await getPool();
    const result = await dbPool.request().query(`
      SELECT
        t.TenantId as id,
        t.Name as name,
        t.DisplayName as displayName,
        COALESCE(t.Color, '#10B981') as color,
        COUNT(DISTINCT f.FarmId) as farmCount,
        COALESCE(SUM(h.CurrentBirds), 0) as totalBirds
      FROM Tenants t
      LEFT JOIN Farms f ON t.TenantId = f.TenantId AND f.IsActive = 1
      LEFT JOIN Houses h ON f.FarmId = h.FarmId AND h.IsActive = 1
      WHERE t.IsActive = 1 AND COALESCE(t.IsDisplayed, 1) = 1
      GROUP BY t.TenantId, t.Name, t.DisplayName, t.Color
      ORDER BY t.Name
    `);

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

app.http("getTenants", {
  methods: ["GET"],
  route: "tenants",
  authLevel: "anonymous",
  handler: getTenants,
});

// GET /user/permissions - Get user's tenant permissions
async function getUserPermissions(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const email = request.query.get("email");

  if (!email) {
    return { status: 400, body: "Email parameter required" };
  }

  try {
    const dbPool = await getPool();
    const result = await dbPool.request()
      .input("email", sql.NVarChar, email)
      .query(`
        SELECT
          ut.UserEmail as email,
          ut.Role as role,
          ut.IsGlobalAdmin as isGlobalAdmin,
          t.TenantId as tenantId,
          t.Name as tenantName,
          t.DisplayName as tenantDisplayName
        FROM UserTenants ut
        JOIN Tenants t ON ut.TenantId = t.TenantId
        WHERE ut.UserEmail = @email AND ut.IsActive = 1
      `);

    if (result.recordset.length === 0) {
      // User not found - return default viewer permissions (can see displayed tenants)
      return {
        status: 200,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          email,
          isGlobalAdmin: false,
          tenants: [],
        }),
      };
    }

    const firstRow = result.recordset[0];
    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        email: firstRow.email,
        isGlobalAdmin: firstRow.isGlobalAdmin,
        role: firstRow.role,
        tenants: result.recordset.map((r: any) => ({
          id: r.tenantId,
          name: r.tenantName,
          displayName: r.tenantDisplayName,
          role: r.role,
        })),
      }),
    };
  } catch (error) {
    context.log(`Error fetching user permissions: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

app.http("getUserPermissions", {
  methods: ["GET"],
  route: "user/permissions",
  authLevel: "anonymous",
  handler: getUserPermissions,
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

// Register Admin endpoints (use "manage" not "admin" - Azure reserves /admin/ for internal use)
app.http("getAdminTenants", {
  methods: ["GET"],
  route: "manage/tenants",
  authLevel: "anonymous",
  handler: getAdminTenants,
});

app.http("updateTenant", {
  methods: ["PUT"],
  route: "manage/tenants/{id}",
  authLevel: "anonymous",
  handler: updateTenant,
});

app.http("getAdminFarms", {
  methods: ["GET"],
  route: "manage/farms",
  authLevel: "anonymous",
  handler: getAdminFarms,
});

app.http("updateFarm", {
  methods: ["PUT"],
  route: "manage/farms/{id}",
  authLevel: "anonymous",
  handler: updateFarm,
});

app.http("getAdminHouses", {
  methods: ["GET"],
  route: "manage/houses",
  authLevel: "anonymous",
  handler: getAdminHouses,
});

app.http("updateHouse", {
  methods: ["PUT"],
  route: "manage/houses/{id}",
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

// ============================================
// Sensor Event Types & Events Endpoints
// ============================================

// GET /api/event-types - Get all sensor event types
async function getEventTypes(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const category = request.query.get("category") || null;

  try {
    const dbPool = await getPool();
    const req = dbPool.request();

    if (category) {
      req.input("Category", sql.NVarChar, category);
    } else {
      req.input("Category", sql.NVarChar, null);
    }
    req.input("ActiveOnly", sql.Bit, true);

    const result = await req.execute("usp_GetEventTypes");

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.recordset),
    };
  } catch (error) {
    context.log(`Error fetching event types: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// GET /api/houses/:houseId/events - Get latest sensor events for a house
async function getLatestEvents(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const houseId = request.params.houseId;
  const category = request.query.get("category") || null;

  try {
    const dbPool = await getPool();
    const req = dbPool.request()
      .input("HouseId", sql.UniqueIdentifier, houseId);

    if (category) {
      req.input("Category", sql.NVarChar, category);
    } else {
      req.input("Category", sql.NVarChar, null);
    }

    const result = await req.execute("usp_GetLatestSensorEvents");

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.recordset),
    };
  } catch (error) {
    context.log(`Error fetching latest events: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// GET /api/houses/:houseId/events/history - Get event history
async function getEventHistory(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const houseId = request.params.houseId;
  const hours = parseInt(request.query.get("hours") || "24");
  const eventTypes = request.query.get("types") || null;

  try {
    const dbPool = await getPool();
    const req = dbPool.request()
      .input("HouseId", sql.UniqueIdentifier, houseId)
      .input("Hours", sql.Int, hours);

    if (eventTypes) {
      req.input("EventTypes", sql.NVarChar, eventTypes);
    } else {
      req.input("EventTypes", sql.NVarChar, null);
    }

    const result = await req.execute("usp_GetSensorEventHistory");

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.recordset),
    };
  } catch (error) {
    context.log(`Error fetching event history: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// POST /api/events - Insert sensor events (batch)
async function postSensorEvents(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  try {
    const body = await request.json() as { events: unknown[] };
    const eventsJson = JSON.stringify(body.events);

    const dbPool = await getPool();
    const result = await dbPool.request()
      .input("EventsJson", sql.NVarChar, eventsJson)
      .execute("usp_InsertSensorEventBatch");

    return {
      status: 201,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ inserted: result.recordset[0]?.EventsInserted || 0 }),
    };
  } catch (error) {
    context.log(`Error inserting events: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

// GET /api/thresholds - Get thresholds for a specific bird age
async function getThresholds(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const birdAge = parseInt(request.query.get("birdAge") || "21");
  const eventType = request.query.get("eventType") || null;

  try {
    const dbPool = await getPool();
    const req = dbPool.request()
      .input("BirdAgeDays", sql.Int, birdAge);

    if (eventType) {
      req.input("EventTypeCode", sql.NVarChar, eventType);
    } else {
      req.input("EventTypeCode", sql.NVarChar, null);
    }

    const result = await req.execute("usp_GetThresholdsForAge");

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(result.recordset),
    };
  } catch (error) {
    context.log(`Error fetching thresholds: ${error}`);
    return { status: 500, body: "Internal server error" };
  }
}

app.http("getEventTypes", {
  methods: ["GET"],
  route: "event-types",
  authLevel: "anonymous",
  handler: getEventTypes,
});

app.http("getLatestEvents", {
  methods: ["GET"],
  route: "houses/{houseId}/events",
  authLevel: "anonymous",
  handler: getLatestEvents,
});

app.http("getEventHistory", {
  methods: ["GET"],
  route: "houses/{houseId}/events/history",
  authLevel: "anonymous",
  handler: getEventHistory,
});

app.http("postSensorEvents", {
  methods: ["POST"],
  route: "events",
  authLevel: "anonymous",
  handler: postSensorEvents,
});

app.http("getThresholds", {
  methods: ["GET"],
  route: "thresholds",
  authLevel: "anonymous",
  handler: getThresholds,
});

// ============================================
// Weather API (using Open-Meteo - free, no key)
// ============================================

interface WeatherResponse {
  temperature: number;
  humidity: number;
  windSpeed: number;
  condition: string;
  forecast: Array<{
    day: string;
    high: number;
    low: number;
    condition: string;
  }>;
}

async function getWeather(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const lat = parseFloat(request.query.get("lat") || "36.0");
  const lng = parseFloat(request.query.get("lng") || "-79.0");

  try {
    // Fetch current weather and forecast from Open-Meteo
    const url = `https://api.open-meteo.com/v1/forecast?latitude=${lat}&longitude=${lng}&current=temperature_2m,relative_humidity_2m,wind_speed_10m,weather_code&daily=weather_code,temperature_2m_max,temperature_2m_min&temperature_unit=fahrenheit&wind_speed_unit=mph&timezone=America%2FNew_York&forecast_days=3`;

    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Weather API error: ${response.status}`);
    }

    const data = await response.json();

    // Map weather codes to conditions
    const getCondition = (code: number): string => {
      if (code === 0) return "sunny";
      if (code <= 3) return "cloudy";
      if (code <= 67 || (code >= 80 && code <= 82)) return "rainy";
      if (code >= 71 && code <= 77) return "cold";
      return "cloudy";
    };

    const days = ["Today", "Tomorrow", "Day 3"];
    const weather: WeatherResponse = {
      temperature: Math.round(data.current.temperature_2m),
      humidity: Math.round(data.current.relative_humidity_2m),
      windSpeed: Math.round(data.current.wind_speed_10m),
      condition: getCondition(data.current.weather_code),
      forecast: data.daily.time.map((date: string, i: number) => ({
        day: days[i] || date,
        high: Math.round(data.daily.temperature_2m_max[i]),
        low: Math.round(data.daily.temperature_2m_min[i]),
        condition: getCondition(data.daily.weather_code[i]),
      })),
    };

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(weather),
    };
  } catch (error) {
    context.log(`Error fetching weather: ${error}`);
    return { status: 500, body: "Failed to fetch weather data" };
  }
}

app.http("getWeather", {
  methods: ["GET"],
  route: "weather",
  authLevel: "anonymous",
  handler: getWeather,
});

// ============================================
// Equipment Runtime API
// ============================================

async function getEquipmentRuntime(request: HttpRequest, context: InvocationContext): Promise<HttpResponseInit> {
  const tenantId = request.query.get("tenantId") || "arrowfoot";

  try {
    const dbPool = await getPool();

    // Get equipment status events from today
    const result = await dbPool.request()
      .input("TenantId", sql.NVarChar, tenantId)
      .query(`
        WITH TodayEvents AS (
          SELECT
            se.HouseId,
            t.Code as EventType,
            se.BoolValue,
            se.Value,
            se.Timestamp,
            ROW_NUMBER() OVER (PARTITION BY se.HouseId, t.Code ORDER BY se.Timestamp DESC) as rn
          FROM SensorEvents se
          INNER JOIN SensorEventTypes t ON se.EventTypeId = t.EventTypeId
          INNER JOIN Houses h ON se.HouseId = h.HouseId
          INNER JOIN Farms f ON h.FarmId = f.FarmId
          INNER JOIN Tenants tn ON f.TenantId = tn.TenantId
          WHERE tn.Name = @TenantId
            AND se.Timestamp >= CAST(GETUTCDATE() AS DATE)
            AND t.Code IN ('fan_status', 'heater_status', 'fogger_status', 'curtain_position')
        ),
        LatestStatus AS (
          SELECT HouseId, EventType, BoolValue, Value
          FROM TodayEvents
          WHERE rn = 1
        ),
        HouseCounts AS (
          SELECT COUNT(DISTINCT h.HouseId) as TotalHouses
          FROM Houses h
          INNER JOIN Farms f ON h.FarmId = f.FarmId
          INNER JOIN Tenants t ON f.TenantId = t.TenantId
          WHERE t.Name = @TenantId AND h.Status = 'active'
        ),
        EquipmentStats AS (
          SELECT
            EventType,
            COUNT(CASE WHEN BoolValue = 1 OR Value > 50 THEN 1 END) as ActiveCount,
            COUNT(*) as ReportingCount
          FROM LatestStatus
          GROUP BY EventType
        )
        SELECT
          es.EventType,
          es.ActiveCount,
          es.ReportingCount,
          hc.TotalHouses,
          -- Estimate runtime hours based on % of day equipment was active (simplified)
          CASE
            WHEN es.EventType = 'heater_status' THEN es.ActiveCount * 4.5
            WHEN es.EventType = 'fan_status' THEN es.ActiveCount * 6.0
            WHEN es.EventType = 'fogger_status' THEN es.ActiveCount * 2.0
            ELSE es.ActiveCount * 8.0
          END as EstimatedRuntimeHours
        FROM EquipmentStats es
        CROSS JOIN HouseCounts hc
      `);

    // Also get lighting status from sensor readings
    const lightingResult = await dbPool.request()
      .input("TenantId", sql.NVarChar, tenantId)
      .query(`
        SELECT
          COUNT(CASE WHEN se.Value > 5 THEN 1 END) as LightsOn,
          COUNT(*) as TotalHouses
        FROM (
          SELECT h.HouseId,
            (SELECT TOP 1 Value FROM SensorEvents se2
             INNER JOIN SensorEventTypes t ON se2.EventTypeId = t.EventTypeId
             WHERE se2.HouseId = h.HouseId AND t.Code = 'lighting'
             ORDER BY se2.Timestamp DESC) as Value
          FROM Houses h
          INNER JOIN Farms f ON h.FarmId = f.FarmId
          INNER JOIN Tenants t ON f.TenantId = t.TenantId
          WHERE t.Name = @TenantId AND h.Status = 'active'
        ) se
      `);

    // Format response
    const equipmentData = [
      {
        name: "Heaters",
        eventType: "heater_status",
        activeCount: 0,
        totalCount: 0,
        runtimeHours: 0,
        costPerHour: 2.50,
        costUnit: "propane",
      },
      {
        name: "Ventilation",
        eventType: "fan_status",
        activeCount: 0,
        totalCount: 0,
        runtimeHours: 0,
        costPerHour: 1.50,
        costUnit: "electricity",
      },
      {
        name: "Foggers",
        eventType: "fogger_status",
        activeCount: 0,
        totalCount: 0,
        runtimeHours: 0,
        costPerHour: 0.50,
        costUnit: "water",
      },
      {
        name: "Lighting",
        eventType: "lighting",
        activeCount: lightingResult.recordset[0]?.LightsOn || 0,
        totalCount: lightingResult.recordset[0]?.TotalHouses || 0,
        runtimeHours: (lightingResult.recordset[0]?.LightsOn || 0) * 8,
        costPerHour: 0.75,
        costUnit: "electricity",
      },
    ];

    // Merge SQL results
    for (const row of result.recordset) {
      const eq = equipmentData.find(e => e.eventType === row.EventType);
      if (eq) {
        eq.activeCount = row.ActiveCount;
        eq.totalCount = row.TotalHouses;
        eq.runtimeHours = row.EstimatedRuntimeHours;
      }
    }

    // Calculate costs
    const response = equipmentData.map(eq => ({
      ...eq,
      utilizationPercent: eq.totalCount > 0 ? (eq.activeCount / eq.totalCount) * 100 : 0,
      estimatedCost: eq.runtimeHours * eq.costPerHour,
    }));

    return {
      status: 200,
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(response),
    };
  } catch (error) {
    context.log(`Error fetching equipment runtime: ${error}`);
    return { status: 500, body: "Failed to fetch equipment data" };
  }
}

app.http("getEquipmentRuntime", {
  methods: ["GET"],
  route: "equipment/runtime",
  authLevel: "anonymous",
  handler: getEquipmentRuntime,
});
