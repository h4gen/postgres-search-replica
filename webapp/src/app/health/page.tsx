"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Activity } from "lucide-react";

export default function HealthPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight mb-2">Replication Health</h1>
        <p className="text-muted-foreground">Detailed monitoring of replication slots and CDC streams.</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Activity className="size-5 text-primary" />
            Stream Diagnostics
          </CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          Detailed metrics for individual replication slots and outbox workers will appear here.
        </CardContent>
      </Card>
    </div>
  );
}
