export const delay = (min: number, max: number): Promise<void> => {
    return new Promise(resolve => {
        const time = Math.floor(Math.random() * (max - min + 1) + min) * 1000;
        setTimeout(resolve, time);
    });
};