"use client";

import React, { useState, useEffect } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { 
  Card, 
  CardContent, 
  CardDescription, 
  CardHeader, 
  CardTitle 
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { 
  Select, 
  SelectContent, 
  SelectItem, 
  SelectTrigger, 
  SelectValue 
} from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { toast } from "sonner";
import { Save, Play, Beaker, Monitor, Database, GitBranch, Check, X } from "lucide-react";
import { ScrollArea } from "@/components/ui/scroll-area";

export function SettingsManager() {
  const queryClient = useQueryClient();
  const { data: summary } = useQuery({
    queryKey: ["summary"],
    queryFn: api.getSummary,
  });

  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [editConfig, setEditConfig] = useState<any>(null);
  const [dryRunResult, setDryRunResult] = useState<any>(null);

  const { data: schema, isLoading: isSchemaLoading } = useQuery({
    queryKey: ["schema", selectedTable],
    queryFn: () => api.getSchema(selectedTable!),
    enabled: !!selectedTable,
  });

  const updateMutation = useMutation({
    mutationFn: ({ name, config }: { name: string, config: any }) => api.updateConfig(name, config),
    onSuccess: () => {
      toast.success("Configuration applied to PRODUCTION");
      queryClient.invalidateQueries({ queryKey: ["summary"] });
    },
    onError: (err: any) => {
      toast.error(`Update failed: ${err.message}`);
    }
  });

  const dryRunMutation = useMutation({
    mutationFn: ({ name, config }: { name: string, config: any }) => api.getDryRun(name, config),
    onSuccess: (data) => {
      setDryRunResult(data);
      toast.info("Pre-flight analysis complete");
    },
  });

  const handleTableSelect = (name: string) => {
    setSelectedTable(name);
    const tableSummary = summary?.config_summaries?.[name];
    
    // We try to find the full config if available in projections or use summary defaults
    const tableConfig = summary?.pipeline?.vectorizers?.find((v: any) => v.name === name || v.source_table === name);

    setEditConfig({
      source_table: name,
      publication_columns: ["id", "title", "content"], 
      embedding_model: tableSummary?.model || "nomic-embed-text",
      search_profile: tableSummary?.search_profile || "vector",
      active: true
    });
    setDryRunResult(null);
  };

  const toggleColumn = (col: string) => {
    setEditConfig((prev: any) => {
      const cols = prev.publication_columns.includes(col)
        ? prev.publication_columns.filter((c: string) => c !== col)
        : [...prev.publication_columns, col];
      return { ...prev, publication_columns: cols };
    });
  };

  const activeTables = Object.keys(summary?.config_summaries || {});

  return (
    <div className="grid gap-6 md:grid-cols-4 p-6 font-mono max-w-7xl mx-auto">
      <div className="md:col-span-1 space-y-4">
        <div className="flex items-center gap-2 mb-6">
            <GitBranch className="h-5 w-5 text-primary" />
            <h3 className="text-xs font-bold uppercase tracking-widest text-primary">Registry Nodes</h3>
        </div>
        <ScrollArea className="h-[calc(100vh-200px)] pr-4">
            <div className="space-y-2">
                {activeTables.map(name => (
                <Button
                    key={name}
                    variant={selectedTable === name ? "default" : "outline"}
                    className={`w-full justify-start rounded-none border-2 h-14 text-[10px] uppercase font-bold tracking-tighter ${selectedTable === name ? 'border-primary' : 'border-white/10 opacity-60 hover:opacity-100'}`}
                    onClick={() => handleTableSelect(name)}
                >
                    <Database className={`mr-2 h-4 w-4 ${selectedTable === name ? 'text-white' : 'text-primary'}`} />
                    <div className="text-left">
                        <div>{name}</div>
                        <div className="text-[8px] opacity-60 font-mono">GEN {summary?.config_summaries?.[name]?.generation} | {summary?.config_summaries?.[name]?.version_id}</div>
                    </div>
                </Button>
                ))}
            </div>
        </ScrollArea>
      </div>

      <div className="md:col-span-3 space-y-6">
        {selectedTable && editConfig ? (
          <>
            <div className="flex items-center justify-between bg-white/5 p-4 border-2 border-white/10">
                <div>
                     <h2 className="text-xl font-black uppercase tracking-tighter">NODE: {selectedTable}</h2>
                     <p className="text-[10px] font-mono text-muted-foreground">VERSION CONTROL & RECONCILIATION SETTINGS</p>
                </div>
                <div className="flex gap-2">
                    <Badge variant="outline" className="rounded-none border-primary/40 bg-primary/5 text-primary font-mono text-[10px]">
                        LSN: {summary?.pipeline?.source?.current_lsn || 'UNKNOWN'}
                    </Badge>
                    <Badge variant="outline" className="rounded-none border-white/20 font-mono text-[10px]">
                        ID: {summary?.config_summaries?.[selectedTable]?.version_id}
                    </Badge>
                </div>
            </div>

            <div className="grid gap-6 md:grid-cols-2">
                <Card className="rounded-none border-2 bg-transparent">
                <CardHeader className="border-b-2 border-white/5">
                    <CardTitle className="text-xs font-bold uppercase tracking-widest">Model & Strategy</CardTitle>
                </CardHeader>
                <CardContent className="space-y-6 pt-6">
                    <div className="space-y-2">
                        <label className="text-[10px] uppercase font-bold text-primary tracking-widest">Embedding Model</label>
                        <Select 
                        value={editConfig.embedding_model} 
                        onValueChange={(v: string) => setEditConfig({...editConfig, embedding_model: v})}
                        >
                        <SelectTrigger className="rounded-none border-2 border-white/10 bg-black h-12 font-mono text-xs">
                            <SelectValue />
                        </SelectTrigger>
                        <SelectContent className="rounded-none border-2 bg-black font-mono">
                            <SelectItem value="nomic-embed-text">nomic-embed-text (Production)</SelectItem>
                            <SelectItem value="llama3">Llama 3 (Experimental)</SelectItem>
                            <SelectItem value="mistral">Mistral (Fast)</SelectItem>
                        </SelectContent>
                        </Select>
                    </div>

                    <div className="space-y-2">
                        <label className="text-[10px] uppercase font-bold text-primary tracking-widest">Search Strategy</label>
                        <Select 
                        value={editConfig.search_profile} 
                        onValueChange={(v: string) => setEditConfig({...editConfig, search_profile: v})}
                        >
                        <SelectTrigger className="rounded-none border-2 border-white/10 bg-black h-12 font-mono text-xs">
                            <SelectValue />
                        </SelectTrigger>
                        <SelectContent className="rounded-none border-2 bg-black font-mono">
                            <SelectItem value="hybrid">Hybrid (RRF Dense+Sparse)</SelectItem>
                            <SelectItem value="vector">Vector Only (Semantic)</SelectItem>
                            <SelectItem value="keyword">Keyword Only (BM25)</SelectItem>
                        </SelectContent>
                        </Select>
                    </div>
                </CardContent>
                </Card>

                <Card className="rounded-none border-2 bg-transparent">
                <CardHeader className="border-b-2 border-white/5">
                    <CardTitle className="text-xs font-bold uppercase tracking-widest">Source Schema</CardTitle>
                </CardHeader>
                <CardContent className="pt-6">
                    <div className="space-y-2">
                        <label className="text-[10px] uppercase font-bold text-primary tracking-widest mb-2 block">Available Columns</label>
                        <ScrollArea className="h-[140px] border-2 border-white/10 bg-black/40 p-2">
                            {isSchemaLoading ? (
                                <div className="p-4 text-[10px] animate-pulse">DETECTING SCHEMA...</div>
                            ) : schema?.columns ? (
                                <div className="grid grid-cols-1 gap-1">
                                    {Object.entries(schema.columns).map(([col, type]: [string, any]) => (
                                        <div 
                                            key={col} 
                                            className={`flex items-center justify-between p-2 text-[10px] font-mono cursor-pointer transition-colors ${editConfig.publication_columns.includes(col) ? 'bg-primary/20 text-primary' : 'hover:bg-white/5 opacity-50'}`}
                                            onClick={() => toggleColumn(col)}
                                        >
                                            <div className="flex items-center gap-2">
                                                {editConfig.publication_columns.includes(col) ? <Check className="h-3 w-3" /> : <div className="w-3" />}
                                                {col}
                                            </div>
                                            <span className="opacity-40">{type}</span>
                                        </div>
                                    ))}
                                </div>
                            ) : (
                                <div className="p-4 text-[10px] text-red-400">FAILED TO CONNECT TO SOURCE</div>
                            )}
                        </ScrollArea>
                        <p className="text-[8px] text-muted-foreground mt-2">Selected columns will be mirrored to the sink for search indexing.</p>
                    </div>
                </CardContent>
                </Card>
            </div>

            <div className="flex gap-4 p-6 bg-black border-2 border-primary/20">
                <div className="flex-1 flex flex-col gap-2">
                    <Button 
                        variant="ghost" 
                        className="rounded-none border-2 border-white/10 uppercase text-xs h-14 hover:bg-primary/10 hover:text-primary transition-all"
                        onClick={() => dryRunMutation.mutate({ name: selectedTable, config: editConfig })}
                        disabled={dryRunMutation.isPending}
                    >
                        <Beaker className="mr-2 h-5 w-5" />
                        Run Pre-flight Diagnostic
                    </Button>
                    <p className="text-[9px] text-center opacity-40">Preview performance and cost impacts</p>
                </div>
                <div className="flex-1 flex flex-col gap-2">
                    <Button 
                        className="rounded-none uppercase text-xs h-14 bg-primary hover:bg-primary/90 font-black tracking-widest shadow-[0_0_20px_rgba(var(--primary-rgb),0.3)]"
                        onClick={() => updateMutation.mutate({ name: selectedTable, config: editConfig })}
                        disabled={updateMutation.isPending}
                    >
                        <Save className="mr-2 h-5 w-5" />
                        Promote to Production
                    </Button>
                    <p className="text-[9px] text-center text-primary font-bold opacity-80">Triggers Atomic Blue-Green Swap</p>
                </div>
            </div>

            {dryRunResult && (
              <Card className="rounded-none border-2 bg-primary/5 border-primary/20 animate-in fade-in slide-in-from-bottom-2">
                <CardHeader className="border-b-2 border-primary/10">
                  <CardTitle className="text-xs font-bold uppercase flex items-center gap-2 text-primary">
                    <Play className="size-4" />
                    Infrastructure Projection: GEN {(summary?.config_summaries?.[selectedTable]?.generation || 0) + 1}
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4 pt-6">
                  <div className="grid grid-cols-3 gap-4 text-center">
                    <div className="p-4 border-2 bg-black border-white/5">
                      <div className="text-[9px] text-muted-foreground uppercase mb-1 font-bold">Estimated RAM Usage</div>
                      <div className="text-xl font-black text-primary">{dryRunResult.projections?.estimated_ram_mb?.toFixed(2)} <span className="text-xs">MB</span></div>
                    </div>
                    <div className="p-4 border-2 bg-black border-white/5">
                      <div className="text-[9px] text-muted-foreground uppercase mb-1 font-bold">Vector Dimension</div>
                      <div className="text-xl font-black text-primary">{dryRunResult.projections?.dimension || "N/A"}</div>
                    </div>
                    <div className="p-4 border-2 bg-black border-white/5">
                      <div className="text-[9px] text-muted-foreground uppercase mb-1 font-bold">Planned Actions</div>
                      <div className="text-xl font-black text-primary">{dryRunResult.actions?.length || 0} <span className="text-xs">STEPS</span></div>
                    </div>
                  </div>
                  
                  {dryRunResult.actions?.length > 0 && (
                    <div className="text-[10px] bg-black p-4 text-primary border-2 border-primary/20 font-mono leading-relaxed overflow-x-auto max-h-[150px]">
                      {dryRunResult.actions.map((a: string, i: number) => (
                        <div key={i}>&gt; {a}</div>
                      ))}
                    </div>
                  )}
                </CardContent>
              </Card>
            )}
          </>
        ) : (
          <div className="h-[calc(100vh-180px)] flex flex-col items-center justify-center border-2 border-dashed border-white/10 text-muted-foreground bg-white/5">
            <Monitor className="h-16 w-16 mb-6 opacity-10" />
            <h3 className="text-sm font-bold uppercase tracking-[0.3em] opacity-40">System Idle</h3>
            <p className="text-[10px] uppercase mt-2 opacity-30">Select a target registry node to begin configuration</p>
          </div>
        )}
      </div>
    </div>
  );
}
