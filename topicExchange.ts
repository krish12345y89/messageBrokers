import amqplib, { Connection, Channel, ConsumeMessage } from "amqplib";

interface TopicConfig {
    uri: string;
    exchangeName: string;
}

class TopicExchange {
    private connection: Connection | null = null;
    private channel: Channel | null = null;
    private exchange: amqplib.Replies.AssertExchange | null = null;
    protected config: TopicConfig;

    constructor(config: TopicConfig) {
        this.config = config;
        this.initialise();
    }

    private async initialise() {
        try {
            this.connection = await amqplib.connect(this.config.uri);
            console.log("Connection established");
            this.channel = await this.connection.createChannel();
            console.log("Channel created");
            this.exchange = await this.channel.assertExchange(this.config.exchangeName, "topic", { durable: true });
            console.log("Exchange asserted successfully");
        } catch (error) {
            console.error("Failed to initialize connection or channel:", error);
            process.exit(1);
        }
    }

    public async sendMessage(data: object, routingKey: string) {
        try {
            if (!this.channel) {
                await this.initialise();
            }

            await this.channel.publish(this.config.exchangeName, routingKey, Buffer.from(JSON.stringify(data)));
            console.log("Message sent successfully to exchange");
        } catch (error) {
            console.error("Failed to send message:", error);
        }
    }

    public async receiveMessage(pattern: string) {
        try {
            if (!this.channel) {
                await this.initialise();
            }

            const q = await this.channel.assertQueue("", { durable: true, exclusive: true });
            console.log(`Queue '${q.queue}' asserted successfully`);

            await this.channel.bindQueue(q.queue, this.config.exchangeName, pattern);
            console.log(`Queue '${q.queue}' bound successfully to exchange with pattern '${pattern}'`);

            await this.channel.consume(q.queue, (data: ConsumeMessage | null) => {
                if (data) {
                    const consumed = JSON.parse(data.content.toString());
                    console.log("Data consumed successfully:", consumed);
                    this.channel.ack(data);
                }
            }, { noAck: false });
        } catch (error) {
            console.error("Failed to receive message:", error);
        }
    }

    public async close() {
        try {
            if (this.channel) {
                await this.channel.close();
                console.log("Channel successfully closed");
            }
            if (this.connection) {
                await this.connection.close();
                console.log("Connection successfully closed");
            }
        } catch (error) {
            console.error("Failed to close connection:", error);
        }
    }
}