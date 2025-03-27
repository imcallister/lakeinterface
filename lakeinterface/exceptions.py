class LakeError(Exception):
    """Base exception for all LakeInterface errors"""
    pass

class S3ObjectNotFound(LakeError):
    """Raised when an S3 object cannot be found"""
    pass

class UnsupportedFileType(LakeError):
    """Raised when attempting to handle an unsupported file type"""
    pass

class MetadataError(LakeError):
    """Raised when there are issues with S3 object metadata"""
    pass 