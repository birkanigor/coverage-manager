export const SECRET_KEY = '13f8bf9e98b376265d96fc78c198a7263ffdcff7d90c4cb156f1b871a45a222c';

export interface Session {
    ip: string;
    appId: string;
}

export const sessions: Record<string, Session> = {};

export interface User {
    user_name: string;
    password: string;
}

export const USER: User = { user_name: 'tom', password: 'abc123!' };
