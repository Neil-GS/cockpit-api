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
