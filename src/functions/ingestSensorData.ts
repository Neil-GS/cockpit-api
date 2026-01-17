import { app, EventHubHandler } from "@azure/functions";
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
    console.log("Connected to SQL Database");
  }
  return pool;
}

interface SensorMessage {
  deviceId: string;
  farmId: string;
  houseNumber: number;
  timestamp: string;
  birdAgeInDays: number;
  currentBirds: number;
  capacity: number;
  flockId: string;
  sensors: {
    temperature: number;
    humidity: number;
    ammonia: number;
    co2: number;
    feedLevel: number;
    waterFlow: number;
    ventilation: number;
    lighting: number;
  };
  alerts: Array<{
    type: string;
    severity: string;
    metric: string;
    value: number;
    threshold: number;
    message: string;
  }>;
}

const ingestSensorData: EventHubHandler = async (messages, context) => {
  context.log(`Received ${Array.isArray(messages) ? messages.length : 1} IoT Hub messages`);

  const messageArray = Array.isArray(messages) ? messages : [messages];

  try {
    const dbPool = await getPool();

    for (const message of messageArray) {
      const data = message as SensorMessage;

      // Insert sensor reading
      await dbPool.request()
        .input("houseId", sql.NVarChar, data.deviceId)
        .input("timestamp", sql.DateTime2, new Date(data.timestamp))
        .input("temperature", sql.Decimal(5, 2), data.sensors.temperature)
        .input("humidity", sql.Decimal(5, 2), data.sensors.humidity)
        .input("ammonia", sql.Decimal(5, 2), data.sensors.ammonia)
        .input("co2", sql.Int, data.sensors.co2)
        .input("feedLevel", sql.Decimal(5, 2), data.sensors.feedLevel)
        .input("waterFlow", sql.Decimal(5, 2), data.sensors.waterFlow)
        .input("ventilation", sql.Int, data.sensors.ventilation)
        .input("lighting", sql.Decimal(5, 2), data.sensors.lighting)
        .query(`
          INSERT INTO SensorReadings (
            HouseId, Timestamp, Temperature, Humidity, Ammonia, CO2,
            FeedLevel, WaterFlow, Ventilation, Lighting
          )
          SELECT @houseId, @timestamp, @temperature, @humidity, @ammonia, @co2,
                 @feedLevel, @waterFlow, @ventilation, @lighting
          WHERE EXISTS (SELECT 1 FROM Houses WHERE HouseId = @houseId)
        `);

      // Update house current state
      await dbPool.request()
        .input("houseId", sql.NVarChar, data.deviceId)
        .input("currentBirds", sql.Int, data.currentBirds)
        .input("birdAgeInDays", sql.Int, data.birdAgeInDays)
        .input("flockId", sql.NVarChar, data.flockId)
        .query(`
          UPDATE Houses
          SET CurrentBirds = @currentBirds,
              BirdAgeInDays = @birdAgeInDays,
              FlockId = @flockId,
              UpdatedAt = GETUTCDATE()
          WHERE HouseId = @houseId
        `);

      // Insert alerts
      for (const alert of data.alerts) {
        await dbPool.request()
          .input("houseId", sql.NVarChar, data.deviceId)
          .input("type", sql.NVarChar, alert.type)
          .input("severity", sql.NVarChar, alert.severity)
          .input("metric", sql.NVarChar, alert.metric)
          .input("value", sql.Decimal(10, 2), alert.value)
          .input("threshold", sql.Decimal(10, 2), alert.threshold)
          .input("message", sql.NVarChar, alert.message)
          .query(`
            INSERT INTO Alerts (HouseId, Type, Severity, Metric, Value, Threshold, Message, IsActive)
            SELECT @houseId, @type, @severity, @metric, @value, @threshold, @message, 1
            WHERE EXISTS (SELECT 1 FROM Houses WHERE HouseId = @houseId)
          `);
      }

      context.log(`Processed message from ${data.deviceId}`);
    }
  } catch (error) {
    context.log(`Error processing messages: ${error}`);
    throw error;
  }
};

app.eventHub("ingestSensorData", {
  connection: "IoTHubConnection",
  eventHubName: "%EventHubName%",
  cardinality: "many",
  handler: ingestSensorData,
});
