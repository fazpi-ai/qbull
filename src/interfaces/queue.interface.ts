export interface IJobData {
    id: number;
    data: any;
    groupName: string;
    progress: (value: number) => Promise<void>;
}