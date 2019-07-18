package storage

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/terraform-providers/terraform-provider-azurerm/azurerm/utils"
	"github.com/tombuildsstuff/giovanni/storage/2018-11-09/blob/blobs"
)

const pollingInterval = 10 * time.Second

type CreateBlob struct {
	AccountName   string
	BlobName      string
	ContainerName string
	ContentType   string
	MetaData      map[string]string
	Size          int64
	Source        string
	SourceURI     string
	Type          string
}

func (cb CreateBlob) Create(ctx context.Context, client *blobs.Client) error {
	if cb.SourceURI != "" {
		return cb.createFromRemoteFile(ctx, client)
	}

	if cb.Source != "" {
		return cb.createFromLocalFile(ctx, client)
	}

	return cb.createEmptyFile(ctx, client)
}

func (cb CreateBlob) createFromRemoteFile(ctx context.Context, client *blobs.Client) error {
	if strings.EqualFold(cb.Type, "append") {
		return fmt.Errorf("Append Blobs cannot be uploaded from existing files")
	}

	input := blobs.CopyInput{
		CopySource: cb.SourceURI,
		MetaData:   cb.MetaData,
	}
	if err := client.CopyAndWait(ctx, cb.AccountName, cb.ContainerName, cb.BlobName, input, pollingInterval); err != nil {
		return fmt.Errorf("Error copying %q to Blob %q (Container %q / Account %q): %s", cb.SourceURI, cb.BlobName, cb.ContainerName, cb.AccountName, err)
	}

	return nil
}

func (cb CreateBlob) createFromLocalFile(ctx context.Context, client *blobs.Client) error {
	if strings.EqualFold(cb.Type, "append") {
		return fmt.Errorf("Append Blobs cannot be uploaded from local files")
	}

	file, err := os.Open(cb.Source)
	if err != nil {
		return fmt.Errorf("Error opening %q: %s", cb.Source, err)
	}

	if strings.EqualFold(cb.Type, "block") {
		input := blobs.PutBlockBlobInput{
			ContentType: utils.String(cb.ContentType),
			MetaData:    cb.MetaData,
		}
		if err := client.PutBlockBlobFromFile(ctx, cb.AccountName, cb.ContainerName, cb.BlobName, file, input); err != nil {
			return fmt.Errorf("Error uploading Block Blob %q (Container %q / Account %q) from %q: %s", cb.BlobName, cb.ContainerName, cb.AccountName, cb.Source, err)
		}

		return nil
	} else if strings.EqualFold(cb.Type, "page") {
		// TODO: a friendly method in the SDK
		// first we have to create the Page Block and then append things to it
		//client.PutPageBlob()
		return fmt.Errorf("Not Yet Implemented!")
	}

	return fmt.Errorf("Unsupported Local File Blob Type %q", cb.Type)
}

func (cb CreateBlob) createEmptyFile(ctx context.Context, client *blobs.Client) error {
	if strings.EqualFold(cb.Type, "append") {
		input := blobs.PutAppendBlobInput{
			ContentType: utils.String(cb.ContentType),
			MetaData:    cb.MetaData,
			// TODO: Content Size
		}
		if _, err := client.PutAppendBlob(ctx, cb.AccountName, cb.ContainerName, cb.BlobName, input); err != nil {
			return fmt.Errorf("Error creating Append Blob %q (Container %q / Account %q): %s", cb.BlobName, cb.ContainerName, cb.AccountName, err)
		}

		return nil
	}

	if strings.EqualFold(cb.Type, "block") {
		input := blobs.PutBlockBlobInput{
			ContentType: utils.String(cb.ContentType),
			MetaData:    cb.MetaData,
		}
		if _, err := client.PutBlockBlob(ctx, cb.AccountName, cb.ContainerName, cb.BlobName, input); err != nil {
			return fmt.Errorf("Error creating Empty Block Blob %q (Container %q / Account %q): %s", cb.BlobName, cb.ContainerName, cb.AccountName, err)
		}

		return nil
	}

	if strings.EqualFold(cb.Type, "page") {
		input := blobs.PutPageBlobInput{
			BlobContentLengthBytes: cb.Size,
			ContentType:            utils.String(cb.ContentType),
			MetaData:               cb.MetaData,
		}
		if _, err := client.PutPageBlob(ctx, cb.AccountName, cb.ContainerName, cb.BlobName, input); err != nil {
			return fmt.Errorf("Error creating Empty Page Blob %q (Container %q / Account %q): %s", cb.BlobName, cb.ContainerName, cb.AccountName, err)
		}

		return nil
	}

	return fmt.Errorf("Unsupported Empty Blob Type %q", cb.Type)
}
