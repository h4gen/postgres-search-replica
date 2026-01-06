export const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

export interface TableConfig {
  search_profile?: string;
  embedding_model?: string;
  version_id?: string;
  [key: string]: any;
}

export interface Summary {
  status: string;
  pipeline: any;
  projections: Record<string, any>;
  event_log: Array<{
    target_name: string;
    generation: number;
    status: string;
    error_message: string | null;
    created_at: string;
  }>;
  config_summaries: Record<string, {
    search_profile: string;
    model: string;
    version_id: string;
    generation: number;
  }>;
}


export interface SchemaResponse {
  target_name: string;
  columns: Record<string, string>;
}

export interface DryRunResponse {
  target_name: string;
  actions: string[];
  projections: any;
}

export const api = {
  async getSummary(): Promise<Summary> {
    const res = await fetch(`${API_BASE_URL}/control-plane/summary`);
    if (!res.ok) throw new Error('Failed to fetch summary');
    return res.json();
  },

  async getDryRun(targetName: string, config?: TableConfig): Promise<DryRunResponse> {
    const url = new URL(`${API_BASE_URL}/control-plane/dry-run/${targetName}`);
    const res = await fetch(url.toString(), {
      method: 'POST', // or GET depending on FastAPI implementation
      headers: { 'Content-Type': 'application/json' },
      body: config ? JSON.stringify(config) : undefined,
    });
    if (!res.ok) throw new Error('Failed to fetch dry run');
    return res.json();
  },

  async updateConfig(targetName: string, config: TableConfig) {
    const res = await fetch(`${API_BASE_URL}/control-plane/config/${targetName}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(config),
    });
    if (!res.ok) throw new Error('Failed to update config');
    return res.json();
  },

  async getHealth() {
    const res = await fetch(`${API_BASE_URL}/health`);
    if (!res.ok) throw new Error('Health check failed');
    return res.json();
  },


  async getSchema(targetName: string): Promise<SchemaResponse> {
    const res = await fetch(`${API_BASE_URL}/control-plane/schema/${targetName}`);
    if (!res.ok) throw new Error('Failed to fetch schema');
    return res.json();
  }
};
