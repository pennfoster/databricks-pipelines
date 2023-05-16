SET
    ANSI_NULLS ON
GO
SET
    QUOTED_IDENTIFIER ON
GO
    CREATE TABLE [dbo].[T022_GoogleNonSearchAds](
        [C022_QueryName] [varchar](50) NULL,
        [C022_Date] [date] NULL,
        [C022_Year] [varchar](255) NULL,
        [C022_YearMonth] [varchar](255) NULL,
        [C022_Month] [varchar](255) NULL,
        [C022_DayofMonth] [varchar](255) NULL,
        [C022_DayofWeek] [varchar](255) NULL,
        [C022_AdID] [varchar](255) NULL,
        [C022_FinalURL] [varchar](max) NULL,
        [C022_Account] [varchar](255) NULL,
        [C022_AccountID] [varchar](255) NULL,
        [C022_Network] [varchar](255) NULL,
        [C022_CampaignName] [varchar](255) NULL,
        [C022_CampaignID] [varchar](255) NULL,
        [C022_CampaignStatus] [varchar](255) NULL,
        [C022_AdGroupName] [varchar](255) NULL,
        [C022_AdGroupID] [varchar](255) NULL,
        [C022_AdGroupStatus] [varchar](255) NULL,
        [C022_Headline] [varchar](255) NULL,
        [C022_HeadlinePart1] [varchar](255) NULL,
        [C022_HeadlinePart2] [varchar](255) NULL,
        [C022_HeadlinePart3] [varchar](255) NULL,
        [C022_Path1] [varchar](255) NULL,
        [C022_Path2] [varchar](255) NULL,
        [C022_Description] [varchar](255) NULL,
        [C022_Description1] [varchar](255) NULL,
        [C022_Description2] [varchar](255) NULL,
        [C022_ExpandedTextAdDescription1] [varchar](255) NULL,
        [C022_ExpandedTextAdDescription2] [varchar](255) NULL,
        [C022_AdType] [varchar](255) NULL,
        [C022_AdStatus] [varchar](255) NULL,
        [C022_TrackingURLTemplate] [varchar](max) NULL,
        [C022_CustomURLParameters] [varchar](max) NULL,
        [C022_ImageAdName] [varchar](255) NULL,
        [C022_CampaignLabels] [varchar](255) NULL,
        [C022_AdGroupLabels] [varchar](255) NULL,
        [C022_AdLabels] [varchar](255) NULL,
        [C022_Labels] [varchar](255) NULL,
        [C022_Headline1] [varchar](255) NULL,
        [C022_Headline2] [varchar](255) NULL,
        [C022_Headline3] [varchar](255) NULL,
        [C022_Headline4] [varchar](255) NULL,
        [C022_Headline5] [varchar](255) NULL,
        [C022_LongHeadline] [varchar](255) NULL,
        [C022_Description1MARD] [varchar](255) NULL,
        [C022_Description2MARD] [varchar](255) NULL,
        [C022_Description3MARD] [varchar](255) NULL,
        [C022_Description4MARD] [varchar](255) NULL,
        [C022_Description5MARD] [varchar](255) NULL,
        [C022_CallToActionText] [varchar](255) NULL,
        [C022_Impressions] [float] NULL,
        [C022_Clicks] [float] NULL,
        [C022_Cost] [float] NULL,
        [C022_CTR] [varchar](255) NULL,
        [C022_CPC] [float] NULL,
        [C022_CPM] [float] NULL,
        [C022_Conversions] [float] NULL,
        [C022_ConversionRate] [varchar](255) NULL,
        [C022_ConversionsPerImpression] [varchar](255) NULL,
        [C022_CostPerConversion] [float] NULL,
        [C022_ValuePerConversion] [float] NULL,
        [C022_TotalConversionValue] [float] NULL,
        [C022_ReturnOnAdSpend] [float] NULL,
        [C022_ViewThroughConversions] [float] NULL,
        [C022_AllConversions] [float] NULL,
        [C022_AllConversionValue] [float] NULL,
        [C022_VideoImpressions] [float] NULL,
        [C022_VideoViews] [float] NULL,
        [C022_VideoViewRate] [varchar](255) NULL,
        [C022_Watch25PercentRate] [varchar](255) NULL,
        [C022_Watch50PercentRate] [varchar](255) NULL,
        [C022_Watch75PercentRate] [varchar](255) NULL,
        [C022_Watch100PercentRate] [varchar](255) NULL,
        [C022_Watch25PercentViews] [float] NULL,
        [C022_Watch50PercentViews] [float] NULL,
        [C022_Watch75PercentViews] [float] NULL,
        [C022_Watch100PercentViews] [float] NULL,
        [C022_GmailForwards] [float] NULL,
        [C022_GmailSaves] [float] NULL,
        [C022_GmailSecondaryClicks] [float] NULL,
        [C022_Mkwid] [varchar](255) NULL,
        [C022_Pubcode] [varchar](255) NULL,
        [DateRecordAddedToTable] [datetime2](7) NULL,
        [C022_Adkey] [varchar](255) NULL,
        [C022_LandingPageURL] [varchar](max) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE
    [dbo].[T022_GoogleNonSearchAds]
ADD
    CONSTRAINT [DF__T022_Goog__DateR__61316BF4] DEFAULT (getdate()) FOR [DateRecordAddedToTable]
GO