SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE TABLE [dbo].[T023_GoogleSearchAds](
        [C023_QueryName] [varchar](50) NULL,
        [C023_Date] [date] NULL,
        [C023_Year] [varchar](255) NULL,
        [C023_YearMonth] [varchar](255) NULL,
        [C023_Month] [varchar](255) NULL,
        [C023_DayofMonth] [varchar](255) NULL,
        [C023_DayofWeek] [varchar](255) NULL,
        [C023_AdID] [varchar](255) NULL,
        [C023_CustomURLParameters] [varchar](255) NULL,
        [C023_Account] [varchar](255) NULL,
        [C023_AccountID] [varchar](255) NULL,
        [C023_Network] [varchar](255) NULL,
        [C023_CampaignName] [varchar](255) NULL,
        [C023_CampaignID] [varchar](255) NULL,
        [C023_CampaignStatus] [varchar](255) NULL,
        [C023_AdGroupName] [varchar](255) NULL,
        [C023_AdGroupID] [varchar](255) NULL,
        [C023_AdGroupStatus] [varchar](255) NULL,
        [C023_Headline] [varchar](255) NULL,
        [C023_HeadlinePart1] [varchar](255) NULL,
        [C023_HeadlinePart2] [varchar](255) NULL,
        [C023_HeadlinePart3] [varchar](255) NULL,
        [C023_Path1] [varchar](255) NULL,
        [C023_Path2] [varchar](255) NULL,
        [C023_Description] [varchar](255) NULL,
        [C023_Description1] [varchar](255) NULL,
        [C023_Description2] [varchar](255) NULL,
        [C023_ExpandedTextAdDescription1] [varchar](255) NULL,
        [C023_ExpandedTextAdDescription2] [varchar](255) NULL,
        [C023_AdType] [varchar](255) NULL,
        [C023_AdStatus] [varchar](255) NULL,
        [C023_FinalURL] [varchar](max) NULL,
        [C023_TrackingURLTemplate] [varchar](max) NULL,
        [C023_CampaignLabels] [varchar](255) NULL,
        [C023_AdGroupLabels] [varchar](255) NULL,
        [C023_AdLabels] [varchar](255) NULL,
        [C023_Labels] [varchar](255) NULL,
        [C023_Impressions] [int] NULL,
        [C023_Clicks] [int] NULL,
        [C023_Cost] [float] NULL,
        [C023_CTR] [varchar](255) NULL,
        [C023_CPC] [float] NULL,
        [C023_CPM] [float] NULL,
        [C023_Conversions] [float] NULL,
        [C023_ConversionRate] [varchar](255) NULL,
        [C023_ConversionsPerImpression] [varchar](255) NULL,
        [C023_CostPerConversion] [float] NULL,
        [C023_ValuePerConversion] [float] NULL,
        [C023_TotalConversionValue] [float] NULL,
        [C023_ReturnOnAdSpend] [float] NULL,
        [C023_ViewThroughConversions] [float] NULL,
        [C023_AllConversions] [float] NULL,
        [C023_AllConversionValue] [float] NULL,
        [C023_Mkwid] [varchar](255) NULL,
        [C023_Pubcode] [varchar](255) NULL,
        [DateRecordAddedToTable] [datetime2](7) NULL,
        [C023_Adkey] [varchar](255) NULL,
        [C023_LandingPageURL] [varchar](max) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE
    [dbo].[T023_GoogleSearchAds]
ADD
    CONSTRAINT [DF__T023_Goog__DateR__6501FCD8] DEFAULT (getdate()) FOR [DateRecordAddedToTable]
GO