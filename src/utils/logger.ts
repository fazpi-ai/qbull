import pino from 'pino';

export class Logger {
  private logger;

  constructor(level: string) {
    this.logger = pino({ level });
  }

  info(message: string) {
    this.logger.info(`\n${message}\n`);
  }

  error(message: string) {
    this.logger.error(`\n${message}\n`);
  }

  warn(message: string) {
    this.logger.warn(`\n${message}\n`);
  }
}