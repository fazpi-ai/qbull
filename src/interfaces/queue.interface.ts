export interface IJobData {
    id: string;
    data: any;
    groupName: string;
    progress: (value: number) => Promise<void>;
}