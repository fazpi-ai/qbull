import pino from 'pino';

export class Logger {
  private logger;

  constructor(level: string) {
    this.logger = pino({ level });
  }

  info(message: string) {
    this.logger.info(message);
  }

  error(message: string) {
    this.logger.error(message);
  }

  warn(message: string) {
    this.logger.warn(message);
  }
}