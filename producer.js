const { kafka } = require("./client");
const readline = require("readline");

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

async function init() {
  const producer = kafka.producer();

  console.log("Connecting Producer...");
  await producer.connect();
  console.log("Producer Connected Successfully!");

  rl.setPrompt("> ");
  rl.prompt();

  rl.on("line", async function (line) {
    const [riderName, location] = line.split(" ");
    
    // 1. Safeguard against empty lines or missing values
    if (!riderName || !location) {
      console.log("⚠️ Please enter format: [RiderName] [Location]");
      //rl.prompt();
      
    }
     rl.prompt();

    try {
      // 2. Route payload conditionally to partition 0 or 1
      await producer.send({
        topic: "rider-updates",
        messages: [
          {
            partition: location.toLowerCase() === "north" ? 0 : 1,
            key: "location-update",
            value: JSON.stringify({ name: riderName, location }),
          },
        ],
      });
      console.log(`✅ Dispatched ${riderName} (${location}) to broker.`);
    } catch (error) {
      console.error("❌ Failed to send message:", error);
    }

    // 3. Keep the prompt open for your next manual entry
    rl.prompt();
  }).on("close", async () => {
    console.log("\nDisconnecting Producer...");
    await producer.disconnect();
    console.log("Producer Disconnected. Goodbye!");
    process.exit(0);
  });
}

init().catch(console.error);