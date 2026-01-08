from src.pg_replica.config import (
    SearchPipeline, IngestConfig, PipelineConfig, StorageConfig, ServeConfig,
    ChunkingConfig, EmbeddingConfig, PostgresStoreConfig, SearchProfile
)

class SearchPipelineFactory:
    @staticmethod
    def create(
        table_name="test_table",
        columns=None,
        template="Content: $chunk",
        embedding_provider="openai",
        embedding_model="text-embedding-3-small",
        embedding_dim=1536,
        hybrid=True,
        profiles=None
    ) -> SearchPipeline:
        
        if columns is None:
            columns = ["id", "content"]
            
        ingest = IngestConfig(table=table_name, columns=columns)
        
        pipeline = PipelineConfig(
            template=template,
            chunking=ChunkingConfig(),
            embedding=EmbeddingConfig(
                provider=embedding_provider,
                model=embedding_model,
                dimension=embedding_dim
            )
        )
        
        storage = StorageConfig(
            postgres=PostgresStoreConfig(profile="hybrid" if hybrid else "vector")
        )
        
        if profiles is None:
            profiles = {"default": SearchProfile(mode="hybrid" if hybrid else "vector")}
            
        serve = ServeConfig(profiles=profiles)
        
        return SearchPipeline(
            ingest=ingest, 
            pipeline=pipeline, 
            storage=storage, 
            serve=serve
        )
