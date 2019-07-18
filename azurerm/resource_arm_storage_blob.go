package azurerm

import (
	"fmt"
	"log"
	"strings"

	oldstorage "github.com/Azure/azure-sdk-for-go/storage"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/helper/validation"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/azure"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/tf"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/helpers/validate"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/internal/services/storage"
	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
	"github.com/tombuildsstuff/giovanni/storage/2018-11-09/blob/blobs"
)

func resourceArmStorageBlob() *schema.Resource {
	return &schema.Resource{
		Create:        resourceArmStorageBlobCreate,
		Read:          resourceArmStorageBlobRead,
		Update:        resourceArmStorageBlobUpdate,
		Delete:        resourceArmStorageBlobDelete,
		MigrateState:  resourceStorageBlobMigrateState,
		SchemaVersion: 1,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
				// TODO: validation
			},

			// TODO: deprecate me
			"resource_group_name": azure.SchemaResourceGroupName(),

			"storage_account_name": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validateArmStorageAccountName,
			},

			"storage_container_name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},

			"type": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
				ValidateFunc: validation.StringInSlice([]string{
					"append",
					"block",
					"page",
				}, true),
			},

			"size": {
				Type:         schema.TypeInt,
				Optional:     true,
				ForceNew:     true,
				Default:      0,
				ValidateFunc: validate.IntDivisibleBy(512),
			},

			"content_type": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "application/octet-stream",
			},

			"source": {
				Type:          schema.TypeString,
				Optional:      true,
				ForceNew:      true,
				ConflictsWith: []string{"source_uri"},
			},

			"source_uri": {
				Type:          schema.TypeString,
				Optional:      true,
				ForceNew:      true,
				ConflictsWith: []string{"source"},
			},

			"url": {
				Type:     schema.TypeString,
				Computed: true,
			},

			"parallelism": {
				// TODO: deprecate me
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      8,
				ForceNew:     true,
				ValidateFunc: validation.IntAtLeast(1),
			},

			"attempts": {
				// TODO: deprecate me
				Type:         schema.TypeInt,
				Optional:     true,
				Default:      1,
				ForceNew:     true,
				ValidateFunc: validation.IntAtLeast(1),
			},

			"metadata": storage.MetaDataSchema(),
		},
	}
}

func resourceArmStorageBlobCreate(d *schema.ResourceData, meta interface{}) error {
	ctx := meta.(*ArmClient).StopContext
	storageClient := meta.(*ArmClient).storage

	blobName := d.Get("name").(string)
	containerName := d.Get("storage_container_name").(string)
	accountName := d.Get("storage_account_name").(string)

	resourceGroup, err := storageClient.FindResourceGroup(ctx, accountName)
	if err != nil {
		return fmt.Errorf("Error locating Resource Group for Storage Account %q: %s", accountName, err)
	}
	if resourceGroup == nil {
		log.Printf("[DEBUG] Unable to locate Resource Group for Storage Account %q - assuming removed & removing from state", accountName)
		d.SetId("")
		return nil
	}

	client, err := storageClient.BlobsClient(ctx, *resourceGroup, accountName)
	if err != nil {
		return fmt.Errorf("Error building Blobs Client for Storage Account %q (Resource Group %q): %s", accountName, *resourceGroup, err)
	}

	blobType := d.Get("type").(string)
	sourceUri := d.Get("source_uri").(string)
	contentType := d.Get("content_type").(string)

	log.Printf("[INFO] Creating blob %q in container %q within storage account %q", blobName, containerName, accountName)

	id := client.GetResourceID(accountName, containerName, blobName)
	if requireResourcesToBeImported && d.IsNewResource() {
		input := blobs.GetPropertiesInput{}
		props, err := client.GetProperties(ctx, accountName, containerName, blobName, input)
		if err != nil {
			if !utils.ResponseWasNotFound(props.Response) {
				return fmt.Errorf("Error checking for existence of Blob %q (Container %q / Account %q): %s", blobName, containerName, accountName, err)
			}
		}
		if !utils.ResponseWasNotFound(props.Response) {
			return tf.ImportAsExistsError("azurerm_storage_blob", id)
		}
	}

	// OLD
	blobClient, accountExists, err := meta.(*ArmClient).getBlobStorageClientForStorageAccount(ctx, *resourceGroup, accountName)
	if err != nil {
		return err
	}
	if !accountExists {
		return fmt.Errorf("Storage Account %q Not Found", accountName)
	}
	container := blobClient.GetContainerReference(containerName)
	blob := container.GetBlobReference(blobName)
	// </OLD>

	if sourceUri != "" {
		options := &oldstorage.CopyOptions{}
		if err := blob.Copy(sourceUri, options); err != nil {
			return fmt.Errorf("Error creating oldstorage blob on Azure: %s", err)
		}
	} else {
		switch strings.ToLower(blobType) {
		case "append":
			options := &oldstorage.PutBlobOptions{}
			if err := blob.PutAppendBlob(options); err != nil {
				return fmt.Errorf("Error creating append blob on Azure: %s", err)
			}

		case "block":
			options := &oldstorage.PutBlobOptions{}
			if err := blob.CreateBlockBlob(options); err != nil {
				return fmt.Errorf("Error creating oldstorage blob on Azure: %s", err)
			}

			source := d.Get("source").(string)
			if source != "" {
				parallelism := d.Get("parallelism").(int)
				attempts := d.Get("attempts").(int)

				if err := resourceArmStorageBlobBlockUploadFromSource(containerName, blobName, source, contentType, blobClient, parallelism, attempts); err != nil {
					return fmt.Errorf("Error creating oldstorage blob on Azure: %s", err)
				}
			}
		case "page":
			source := d.Get("source").(string)
			if source != "" {
				parallelism := d.Get("parallelism").(int)
				attempts := d.Get("attempts").(int)

				if err := resourceArmStorageBlobPageUploadFromSource(containerName, blobName, source, contentType, blobClient, parallelism, attempts); err != nil {
					return fmt.Errorf("Error creating oldstorage blob on Azure: %s", err)
				}
			} else {
				size := int64(d.Get("size").(int))
				options := &oldstorage.PutBlobOptions{}

				blob.Properties.ContentLength = size
				blob.Properties.ContentType = contentType
				if err := blob.PutPageBlob(options); err != nil {
					return fmt.Errorf("Error creating oldstorage blob on Azure: %s", err)
				}
			}
		}
	}

	blob.Metadata = expandStorageAccountBlobMetadata(d)

	opts := &oldstorage.SetBlobMetadataOptions{}
	if err := blob.SetMetadata(opts); err != nil {
		return fmt.Errorf("Error setting metadata for oldstorage blob on Azure: %s", err)
	}

	d.SetId(id)
	return resourceArmStorageBlobRead(d, meta)
}

func resourceArmStorageBlobUpdate(d *schema.ResourceData, meta interface{}) error {
	ctx := meta.(*ArmClient).StopContext
	storageClient := meta.(*ArmClient).storage

	id, err := blobs.ParseResourceID(d.Id())
	if err != nil {
		return err
	}

	resourceGroup, err := storageClient.FindResourceGroup(ctx, id.AccountName)
	if err != nil {
		return fmt.Errorf("Error locating Resource Group for Storage Account %q: %s", id.AccountName, err)
	}
	if resourceGroup == nil {
		return fmt.Errorf("Unable to locate Resource Group for Storage Account %q", id.AccountName)
	}

	client, err := storageClient.BlobsClient(ctx, *resourceGroup, id.AccountName)
	if err != nil {
		return fmt.Errorf("Error building Blobs Client for Storage Account %q (Resource Group %q): %s", id.AccountName, *resourceGroup, err)
	}

	if d.HasChange("content_type") {
		log.Printf("[DEBUG] Updating the Properties for Blob %q (Container %q / Account %q)..", id.BlobName, id.ContainerName, id.AccountName)
		input := blobs.SetPropertiesInput{
			ContentType: utils.String(d.Get("content_type").(string)),
		}
		if _, err := client.SetProperties(ctx, id.AccountName, id.ContainerName, id.BlobName, input); err != nil {
			return fmt.Errorf("Error updating the Properties for Blob %q (Container %q / Account %q): %s", id.BlobName, id.ContainerName, id.AccountName, err)
		}
		log.Printf("[DEBUG] Updated the Properties for Blob %q (Container %q / Account %q)", id.BlobName, id.ContainerName, id.AccountName)
	}

	if d.HasChange("metadata") {
		log.Printf("[DEBUG] Updating the MetaData for Blob %q (Container %q / Account %q)..", id.BlobName, id.ContainerName, id.AccountName)
		metaDataRaw := d.Get("metadata").(map[string]interface{})
		metaData := storage.ExpandMetaData(metaDataRaw)

		input := blobs.SetMetaDataInput{
			MetaData: metaData,
		}
		if _, err := client.SetMetaData(ctx, id.AccountName, id.ContainerName, id.BlobName, input); err != nil {
			return fmt.Errorf("Error updating the MetaData for Blob %q (Container %q / Account %q): %s", id.BlobName, id.ContainerName, id.AccountName, err)
		}
		log.Printf("[DEBUG] Updated the MetaData for Blob %q (Container %q / Account %q)", id.BlobName, id.ContainerName, id.AccountName)
	}

	return nil
}

func resourceArmStorageBlobRead(d *schema.ResourceData, meta interface{}) error {
	ctx := meta.(*ArmClient).StopContext
	storageClient := meta.(*ArmClient).storage

	id, err := blobs.ParseResourceID(d.Id())
	if err != nil {
		return err
	}

	resourceGroup, err := storageClient.FindResourceGroup(ctx, id.AccountName)
	if err != nil {
		return fmt.Errorf("Error locating Resource Group for Storage Account %q: %s", id.AccountName, err)
	}
	if resourceGroup == nil {
		log.Printf("[DEBUG] Unable to locate Resource Group for Storage Account %q - assuming removed & removing from state", id.AccountName)
		d.SetId("")
		return nil
	}

	client, err := storageClient.BlobsClient(ctx, *resourceGroup, id.AccountName)
	if err != nil {
		return fmt.Errorf("Error building Blobs Client for Storage Account %q (Resource Group %q): %s", id.AccountName, *resourceGroup, err)
	}

	input := blobs.GetPropertiesInput{}
	props, err := client.GetProperties(ctx, id.AccountName, id.ContainerName, id.BlobName, input)
	if err != nil {
		if utils.ResponseWasNotFound(props.Response) {
			log.Printf("[DEBUG] Blob %q was not found in Container %q / Account %q - removing from state!", id.BlobName, id.ContainerName, id.AccountName)
			d.SetId("")
			return nil
		}

		return fmt.Errorf("Error retrieving Blob %q (Container %q / Account %q): %s", id.BlobName, id.ContainerName, id.AccountName, err)
	}

	d.Set("name", id.BlobName)
	d.Set("storage_container_name", id.ContainerName)
	d.Set("storage_account_name", id.AccountName)
	d.Set("resource_group_name", resourceGroup)

	d.Set("content_type", props.ContentType)
	d.Set("source_uri", props.CopySource)

	// TODO: expose the other fields

	blobType := strings.ToLower(strings.Replace(string(props.BlobType), "Blob", "", 1))
	d.Set("type", blobType)
	d.Set("url", client.GetResourceID(id.AccountName, id.ContainerName, id.BlobName))

	if err := d.Set("metadata", storage.FlattenMetaData(props.MetaData)); err != nil {
		return fmt.Errorf("Error setting `metadata`: %s", err)
	}

	return nil
}

func resourceArmStorageBlobDelete(d *schema.ResourceData, meta interface{}) error {
	ctx := meta.(*ArmClient).StopContext
	storageClient := meta.(*ArmClient).storage

	id, err := blobs.ParseResourceID(d.Id())
	if err != nil {
		return err
	}

	resourceGroup, err := storageClient.FindResourceGroup(ctx, id.AccountName)
	if err != nil {
		return fmt.Errorf("Error locating Resource Group for Storage Account %q: %s", id.AccountName, err)
	}
	if resourceGroup == nil {
		log.Printf("[DEBUG] Unable to locate Resource Group for Storage Account %q - assuming removed & removing from state", id.AccountName)
		return nil
	}

	client, err := storageClient.BlobsClient(ctx, *resourceGroup, id.AccountName)
	if err != nil {
		return fmt.Errorf("Error building Blobs Client for Storage Account %q (Resource Group %q): %s", id.AccountName, *resourceGroup, err)
	}

	input := blobs.DeleteInput{
		DeleteSnapshots: true,
	}
	if _, err := client.Delete(ctx, id.AccountName, id.ContainerName, id.BlobName, input); err != nil {
		return fmt.Errorf("Error deleting Blob %q (Container %q / Storage Account %q / Resource Group %q): %s", id.BlobName, id.ContainerName, id.AccountName, *resourceGroup, err)
	}

	return nil
}
