import { useEffect, useState, useCallback } from "react";
import { useNavigate } from "react-router";
import { AlertCircle, Play, Trash2, Plus } from "lucide-react";
import {
  listSavedQueries,
  createSavedQuery,
  deleteSavedQuery,
} from "../api/client";
import type { SavedQuery } from "../api/client";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../components/ui/table";
import { Button } from "../components/ui/button";
import { Skeleton } from "../components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "../components/ui/alert";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "../components/ui/dialog";
import { Input } from "../components/ui/input";
import { Label } from "../components/ui/label";
import { PageContainer } from "../components/PageContainer";

export default function QueriesView() {
  const navigate = useNavigate();
  const [queries, setQueries] = useState<SavedQuery[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<SavedQuery | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [createOpen, setCreateOpen] = useState(false);
  const [createName, setCreateName] = useState("");
  const [createQuery, setCreateQuery] = useState("");
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);

  const load = useCallback(async () => {
    try {
      const data = await listSavedQueries();
      setQueries(data);
      setError(null);
    } catch (err: unknown) {
      const message =
        err instanceof Error ? err.message : "Failed to fetch saved queries";
      setError(message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    load();
  }, [load]);

  const handleDelete = useCallback(async () => {
    if (!deleteTarget) return;
    setDeleting(true);
    try {
      await deleteSavedQuery(deleteTarget.id);
      setDeleteTarget(null);
      await load();
    } catch (err: unknown) {
      const message =
        err instanceof Error ? err.message : "Failed to delete query";
      setError(message);
      setDeleteTarget(null);
    } finally {
      setDeleting(false);
    }
  }, [deleteTarget, load]);

  const handleCreate = useCallback(async () => {
    if (!createName.trim() || !createQuery.trim()) return;
    setCreating(true);
    setCreateError(null);
    try {
      await createSavedQuery({ name: createName.trim(), q: createQuery.trim() });
      setCreateOpen(false);
      setCreateName("");
      setCreateQuery("");
      await load();
    } catch (err: unknown) {
      const message =
        err instanceof Error ? err.message : "Failed to create query";
      setCreateError(message);
    } finally {
      setCreating(false);
    }
  }, [createName, createQuery, load]);

  const handleRun = useCallback(
    (q: string) => {
      navigate(`/#q=${encodeURIComponent(q)}`);
    },
    [navigate],
  );

  const newQueryButton = (
    <Button
      variant="outline"
      size="sm"
      onClick={() => {
        setCreateName("");
        setCreateQuery("");
        setCreateError(null);
        setCreateOpen(true);
      }}
    >
      <Plus className="size-4" />
      New Query
    </Button>
  );

  return (
    <PageContainer title="Saved Queries" actions={newQueryButton}>
      {loading ? (
        <div className="flex flex-col gap-2">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-12 w-full rounded-md" />
          ))}
        </div>
      ) : error && queries.length === 0 ? (
        <div className="flex flex-col items-center gap-3 py-16">
          <Alert variant="destructive" className="max-w-md rounded-md">
            <AlertCircle className="size-4" />
            <AlertTitle>Failed to load saved queries</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
          <Button variant="outline" size="sm" onClick={load}>
            Retry
          </Button>
        </div>
      ) : (
        <>
          {error && (
            <Alert variant="destructive" className="mb-4 rounded-md">
              <AlertCircle className="size-4" />
              <AlertDescription>{error}</AlertDescription>
            </Alert>
          )}

          {queries.length === 0 ? (
            <div className="flex flex-col items-center gap-2 py-24 text-center text-sm text-muted-foreground">
              <p className="text-foreground">No saved queries yet.</p>
              <p>Save a query to quickly access your most-used searches.</p>
            </div>
          ) : (
            <div className="overflow-hidden rounded-md border border-border">
              <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-[200px]">Name</TableHead>
              <TableHead>Query</TableHead>
              <TableHead className="w-[120px] text-end">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {queries.map((q) => (
              <TableRow key={q.id}>
                <TableCell className="font-medium">{q.name}</TableCell>
                <TableCell>
                  <code className="text-xs text-muted-foreground">
                    {q.q.length > 80 ? q.q.slice(0, 80) + "..." : q.q}
                  </code>
                </TableCell>
                <TableCell className="text-end">
                  <div className="flex items-center justify-end gap-1">
                    <Button
                      variant="ghost"
                      size="icon-xs"
                      title="Run query"
                      onClick={() => handleRun(q.q)}
                    >
                      <Play className="size-3" />
                    </Button>
                    <Button
                      variant="ghost"
                      size="icon-xs"
                      title="Delete query"
                      onClick={() => setDeleteTarget(q)}
                    >
                      <Trash2 className="size-3" />
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ))}
                </TableBody>
              </Table>
            </div>
          )}
        </>
      )}

      {/* Delete confirmation dialog */}
      <Dialog
        open={deleteTarget !== null}
        onOpenChange={(open) => {
          if (!open) setDeleteTarget(null);
        }}
      >
        <DialogContent className="max-w-sm rounded-md" showCloseButton={false}>
          <DialogHeader>
            <DialogTitle>Delete saved query</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete &quot;{deleteTarget?.name}&quot;?
              This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setDeleteTarget(null)}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              size="sm"
              disabled={deleting}
              onClick={handleDelete}
            >
              {deleting ? "Deleting..." : "Delete"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Create dialog */}
      <Dialog open={createOpen} onOpenChange={setCreateOpen}>
        <DialogContent className="max-w-md rounded-md" showCloseButton={false}>
          <DialogHeader>
            <DialogTitle>Save new query</DialogTitle>
            <DialogDescription>
              Give your query a name so you can find it later.
            </DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-4 py-2">
            <div className="flex flex-col gap-2">
              <Label htmlFor="sq-name">Name</Label>
              <Input
                id="sq-name"
                placeholder="e.g. Error count by service"
                value={createName}
                onInput={(e) =>
                  setCreateName((e.target as HTMLInputElement).value)
                }
                autoFocus
              />
            </div>
            <div className="flex flex-col gap-2">
              <Label htmlFor="sq-query">Query</Label>
              <Input
                id="sq-query"
                placeholder='e.g. level=error | stats count by service'
                className="font-mono text-xs"
                value={createQuery}
                onInput={(e) =>
                  setCreateQuery((e.target as HTMLInputElement).value)
                }
              />
            </div>
            {createError && (
              <Alert variant="destructive" className="rounded-md py-2">
                <AlertCircle className="size-4" />
                <AlertDescription>{createError}</AlertDescription>
              </Alert>
            )}
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              size="sm"
              onClick={() => setCreateOpen(false)}
            >
              Cancel
            </Button>
            <Button
              size="sm"
              disabled={creating || !createName.trim() || !createQuery.trim()}
              onClick={handleCreate}
            >
              {creating ? "Saving..." : "Save"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </PageContainer>
  );
}
