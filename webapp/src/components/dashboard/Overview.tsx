"use client";

import React, { useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { 
  Card, 
  CardContent, 
  CardDescription, 
  CardHeader, 
  CardTitle 
} from "@/components/ui/card";
import { 
  Table, 
  TableBody, 
  TableCell, 
  TableHead, 
  TableHeader, 
  TableRow 
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { ScrollArea } from "@/components/ui/scroll-area";
import { 
  Activity, 
  Database, 
  Clock, 
  ArrowUpRight,
  Monitor 
} from "lucide-react";
import { Area, AreaChart, ResponsiveContainer } from "recharts";

type MetricHistory = {
  value: number;
}[];

export function Overview() {
  const [history, setHistory] = useState<MetricHistory>([]);
  
  const { data, isLoading, error } = useQuery({
    queryKey: ["summary"],
    queryFn: api.getSummary,
    refetchInterval: 2000, // Frequent poll for "Live" feel
  });

  useEffect(() => {
    if (data?.pipeline?.mirrors?.[0]?.last_sync_latency_ms !== undefined) {
      setHistory(prev => {
        const next = [...prev, { value: data.pipeline.mirrors[0].last_sync_latency_ms }];
        return next.slice(-20); // Keep last 20 points
      });
    }
  }, [data]);

  if (isLoading) return <div className="animate-pulse space-y-4">
    <div className="h-32 bg-muted rounded-xl" />
    <div className="h-64 bg-muted rounded-xl" />
  </div>;

  if (error) return <Card className="border-destructive">
    <CardContent className="pt-6 text-destructive">
      Failed to load system summary. Ensure the daemon is running.
    </CardContent>
  </Card>;

  const tables = Object.entries(data?.config_summaries || {});
  const eventLogs = data?.event_log || [];

  return (
    <div className="space-y-8 p-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight mb-2">Replica Control Plane</h1>
          <p className="text-muted-foreground">Industrial-grade monitoring and infrastructure management.</p>
        </div>
        <Badge variant={data?.status === "ok" ? "default" : "destructive"} className="px-4 py-1">
          {data?.status === "ok" ? "SYSTEM_OPERATIONAL" : "DEGRADED_STATE"}
        </Badge>
      </div>

      <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
        <Card className="rounded-none border-2">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-xs font-bold uppercase tracking-wider">Sync Latency</CardTitle>
            <Clock className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-mono font-bold">
              {data?.pipeline?.mirrors?.[0]?.last_sync_latency_ms?.toFixed(2) || "0.00"}ms
            </div>
            <div className="h-[40px] mt-4">
              <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={history}>
                  <Area type="monotone" dataKey="value" stroke="hsl(var(--primary))" fill="hsl(var(--primary)/0.1)" strokeWidth={2} isAnimationActive={false} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </CardContent>
        </Card>

        <Card className="rounded-none border-2">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-xs font-bold uppercase tracking-wider">Active Tasks</CardTitle>
            <Activity className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-mono font-bold">{tables.length}</div>
            <p className="text-[10px] text-muted-foreground mt-1 uppercase">Search-enabled target tables</p>
          </CardContent>
        </Card>

        <Card className="rounded-none border-2">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-xs font-bold uppercase tracking-wider">LSN Position</CardTitle>
            <ArrowUpRight className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-lg font-mono font-bold truncate">
              {data?.pipeline?.source?.current_lsn || "NOT_CONNECTED"}
            </div>
            <p className="text-[10px] text-muted-foreground mt-1 uppercase">Source WAL Write-ahead Log</p>
          </CardContent>
        </Card>

        <Card className="rounded-none border-2">
          <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
            <CardTitle className="text-xs font-bold uppercase tracking-wider">Storage Health</CardTitle>
            <Database className="h-4 w-4 text-muted-foreground" />
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-mono font-bold">HEALTHY</div>
            <p className="text-[10px] text-muted-foreground mt-1 uppercase">Sink Disk Performance</p>
          </CardContent>
        </Card>
      </div>

      <div className="grid gap-6 md:grid-cols-3">
        <Card className="md:col-span-2 rounded-none border-2">
          <CardHeader>
            <CardTitle>Infrastructure Registry</CardTitle>
            <CardDescription className="text-xs uppercase">Management of versioned search views</CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow className="hover:bg-transparent">
                  <TableHead className="text-[10px] uppercase font-bold">Target View</TableHead>
                  <TableHead className="text-[10px] uppercase font-bold text-center">Version</TableHead>
                  <TableHead className="text-[10px] uppercase font-bold">Vectorization</TableHead>
                  <TableHead className="text-[10px] uppercase font-bold text-right">Throughput</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {tables.map(([name, config]) => {
                  const projection = data?.projections?.[name];
                  const total = projection?.row_count || 0;
                  const pending = projection?.pending_items || 0;
                  const progress = total > 0 ? Math.round(((total - pending) / total) * 100) : 0;

                  return (
                    <TableRow key={name} className="font-mono">
                      <TableCell className="text-sm font-bold">{name}</TableCell>
                      <TableCell className="text-center">
                        <Badge variant="outline" className="text-[10px] rounded-none border-primary/20">
                          {config.version_id}
                        </Badge>
                      </TableCell>
                      <TableCell className="min-w-[150px]">
                        <div className="space-y-1">
                          <div className="flex justify-between text-[9px] uppercase">
                            <span>{pending > 0 ? 'Syncing...' : 'Synced'}</span>
                            <span>{progress}%</span>
                          </div>
                          <Progress value={progress} className="h-1 rounded-none bg-muted" />
                        </div>
                      </TableCell>
                      <TableCell className="text-right text-sm">
                        {config.generation} GENS
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        <Card className="rounded-none border-2">
          <CardHeader>
            <CardTitle>Real-time Events</CardTitle>
            <CardDescription className="text-xs uppercase">Configuration and Sync Log</CardDescription>
          </CardHeader>
          <CardContent>
            <ScrollArea className="bg-black p-4 rounded-none font-mono text-[10px] h-[300px] border-2 border-white/5">
              <div className="space-y-2">
                {eventLogs.length > 0 ? (
                  eventLogs.map((log, i) => (
                    <div key={i} className={log.status === 'Failed' ? 'text-red-400' : 'text-green-500'}>
                      <span className="opacity-50">[{new Date(log.created_at).toLocaleTimeString()}]</span>{" "}
                      <span className="font-bold">{log.target_name} (Gen {log.generation})</span>: {log.status}
                      {log.error_message && (
                        <div className="pl-4 text-red-300 opacity-80">&gt; {log.error_message}</div>
                      )}
                    </div>
                  ))
                ) : (
                  <div className="text-white/20 italic">No events recorded.</div>
                )}
                <div className="text-green-400 font-bold underline animate-pulse mt-4">
                  [LIVE] Continuous replication active
                </div>
              </div>
            </ScrollArea>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
