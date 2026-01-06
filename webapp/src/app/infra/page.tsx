"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ShieldAlert } from "lucide-react";

export default function InfraPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight mb-2">Infrastructure</h1>
        <p className="text-muted-foreground">Manage sink databases, search engines, and models.</p>
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <ShieldAlert className="size-5 text-primary" />
            Service Status
          </CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          Visibility into Postgres, Ollama, and Qdrant clusters will appear here.
        </CardContent>
      </Card>
    </div>
  );
}
