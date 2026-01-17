import { app, EventHubHandler, InvocationContext } from "@azure/functions";
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

// Event format from IoT Simulator
interface SensorEvent {
  houseId: string;       // DeviceId like "arrowfoot-farm-1-house-1"
  eventType: string;     // Event type code like "temperature", "humidity"
  timestamp: string;     // ISO timestamp
  value?: number;        // Numeric value
  stringValue?: string;  // String value (for status, etc.)
  boolValue?: boolean;   // Boolean value (for on/off states)
  deviceId: string;      // Sensor device ID
  quality?: number;      // Data quality (0-100)
}

interface IoTMessage {
  events: SensorEvent[];
}

const ingestSensorData: EventHubHandler = async (messages, context: InvocationContext) => {
  const messageArray = Array.isArray(messages) ? messages : [messages];
  context.log(`Processing ${messageArray.length} IoT Hub message(s)`);

  try {
    const dbPool = await getPool();
    let totalEventsInserted = 0;

    for (const message of messageArray) {
      try {
        // Parse the message - could be raw object or string
        const data: IoTMessage = typeof message === 'string'
          ? JSON.parse(message)
          : message as IoTMessage;

        if (!data.events || !Array.isArray(data.events)) {
          context.log(`Skipping message: no events array found`);
          continue;
        }

        // Call stored procedure with events JSON
        const eventsJson = JSON.stringify(data.events);
        const result = await dbPool.request()
          .input("EventsJson", sql.NVarChar, eventsJson)
          .execute("usp_InsertSensorEventBatch");

        const inserted = result.recordset[0]?.EventsInserted || 0;
        totalEventsInserted += inserted;

        context.log(`Inserted ${inserted} events from batch of ${data.events.length}`);

        // Check for threshold violations and create alerts
        await checkThresholdViolations(dbPool, data.events, context);

      } catch (parseError) {
        context.log(`Error parsing message: ${parseError}`);
      }
    }

    context.log(`Total events inserted: ${totalEventsInserted}`);

  } catch (error) {
    context.log(`Error processing messages: ${error}`);
    throw error;
  }
};

// Check for threshold violations and create alerts
async function checkThresholdViolations(
  dbPool: sql.ConnectionPool,
  events: SensorEvent[],
  context: InvocationContext
): Promise<void> {
  // Group events by house to check thresholds
  const eventsByHouse = new Map<string, SensorEvent[]>();
  for (const event of events) {
    const houseEvents = eventsByHouse.get(event.houseId) || [];
    houseEvents.push(event);
    eventsByHouse.set(event.houseId, houseEvents);
  }

  for (const [houseId, houseEvents] of eventsByHouse) {
    try {
      // Get house info for bird age
      const houseResult = await dbPool.request()
        .input("DeviceId", sql.NVarChar, houseId)
        .query(`
          SELECT HouseId, BirdAgeInDays, Name
          FROM Houses
          WHERE DeviceId = @DeviceId OR HouseId = TRY_CAST(@DeviceId AS UNIQUEIDENTIFIER)
        `);

      if (houseResult.recordset.length === 0) continue;

      const house = houseResult.recordset[0];
      const birdAge = house.BirdAgeInDays || 21;

      // Check each event against thresholds
      for (const event of houseEvents) {
        if (event.value === undefined || event.value === null) continue;

        // Get threshold for this event type and bird age
        const thresholdResult = await dbPool.request()
          .input("EventTypeCode", sql.NVarChar, event.eventType)
          .input("BirdAgeDays", sql.Int, birdAge)
          .execute("usp_GetThresholdsForAge");

        if (thresholdResult.recordset.length === 0) continue;

        const threshold = thresholdResult.recordset[0];
        let alertSeverity: string | null = null;
        let alertMessage = "";

        // Check critical thresholds
        if (threshold.criticalMax !== null && event.value > threshold.criticalMax) {
          alertSeverity = "critical";
          alertMessage = `${threshold.name} is critically high: ${event.value}${threshold.unit} (threshold: ${threshold.criticalMax}${threshold.unit})`;
        } else if (threshold.criticalMin !== null && event.value < threshold.criticalMin) {
          alertSeverity = "critical";
          alertMessage = `${threshold.name} is critically low: ${event.value}${threshold.unit} (threshold: ${threshold.criticalMin}${threshold.unit})`;
        }
        // Check warning thresholds
        else if (threshold.warningMax !== null && event.value > threshold.warningMax) {
          alertSeverity = "warning";
          alertMessage = `${threshold.name} is high: ${event.value}${threshold.unit} (threshold: ${threshold.warningMax}${threshold.unit})`;
        } else if (threshold.warningMin !== null && event.value < threshold.warningMin) {
          alertSeverity = "warning";
          alertMessage = `${threshold.name} is low: ${event.value}${threshold.unit} (threshold: ${threshold.warningMin}${threshold.unit})`;
        }

        // Create alert if threshold violated
        if (alertSeverity) {
          await dbPool.request()
            .input("HouseId", sql.UniqueIdentifier, house.HouseId)
            .input("Type", sql.NVarChar, "threshold")
            .input("Severity", sql.NVarChar, alertSeverity)
            .input("Metric", sql.NVarChar, event.eventType)
            .input("Value", sql.Decimal(10, 2), event.value)
            .input("Threshold", sql.Decimal(10, 2),
              alertSeverity === "critical"
                ? (threshold.criticalMax || threshold.criticalMin)
                : (threshold.warningMax || threshold.warningMin))
            .input("Message", sql.NVarChar, alertMessage)
            .query(`
              INSERT INTO Alerts (HouseId, Type, Severity, Metric, Value, Threshold, Message, IsActive)
              VALUES (@HouseId, @Type, @Severity, @Metric, @Value, @Threshold, @Message, 1)
            `);

          context.log(`Alert created: ${alertSeverity} - ${alertMessage}`);
        }
      }
    } catch (err) {
      context.log(`Error checking thresholds for ${houseId}: ${err}`);
    }
  }
}

// Register the IoT Hub / Event Hub trigger
app.eventHub("ingestSensorData", {
  connection: "IoTHubConnection",
  eventHubName: "%EventHubName%",
  cardinality: "many",
  handler: ingestSensorData,
});
