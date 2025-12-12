#include <Storages/StorageSharedMergeTree.h>
#include <Storages/StorageFactory.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/KeyDescription.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void registerStorageSharedMergeTree(StorageFactory & factory)
{
    factory.registerStorage("SharedMergeTree", [](const StorageFactory::Arguments & args)
    {
        StorageInMemoryMetadata metadata;
        metadata.setColumns(args.columns);
        metadata.setComment(args.comment);

        if (!args.storage_def->order_by && !args.storage_def->primary_key)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "You must provide an ORDER BY or PRIMARY KEY expression in the table definition");

        if (args.storage_def->order_by)
            metadata.sorting_key = KeyDescription::getSortingKeyFromAST(args.storage_def->order_by->ptr(), metadata.columns, args.getContext(), {});
        
        if (args.storage_def->primary_key)
             metadata.primary_key = KeyDescription::getKeyFromAST(args.storage_def->primary_key->ptr(), metadata.columns, args.getContext());
        else if (metadata.sorting_key.definition_ast)
             metadata.primary_key = KeyDescription::getKeyFromAST(metadata.sorting_key.definition_ast, metadata.columns, args.getContext());

        if (args.storage_def->partition_by)
            metadata.partition_key = KeyDescription::getKeyFromAST(args.storage_def->partition_by->ptr(), metadata.columns, args.getContext());
        else
            metadata.partition_key = KeyDescription::buildEmptyKey();
        
        if (args.storage_def->sample_by)
             metadata.sampling_key = KeyDescription::getKeyFromAST(args.storage_def->sample_by->ptr(), metadata.columns, args.getContext());
        else
            metadata.sampling_key = KeyDescription::buildEmptyKey();

        MergeTreeData::MergingParams merging_params;
        merging_params.mode = MergeTreeData::MergingParams::Ordinary;

        auto settings = std::make_unique<MergeTreeSettings>(args.getContext()->getMergeTreeSettings());
        settings->loadFromQuery(*args.storage_def, args.getContext(), LoadingStrictnessLevel::ATTACH <= args.mode);

        return std::make_shared<StorageSharedMergeTree>(
            args.table_id,
            args.relative_data_path,
            metadata,
            args.getContext(),
            "", // date_column_name
            merging_params,
            std::move(settings));
    },
    {
        .supports_settings = true,
        .supports_skipping_indices = true,
        .supports_sort_order = true,
        .supports_ttl = true,
        .supports_parallel_insert = true,
        .has_builtin_setting_fn = MergeTreeSettings::hasBuiltin,
    });
}

}
