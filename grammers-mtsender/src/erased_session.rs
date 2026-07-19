// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use grammers_session::types::{DcOption, PeerId, PeerInfo, PeerRef, UpdateState, UpdatesState};
use grammers_session::{BoxFuture, Session};

use crate::errors::ErasedSessionError;

pub trait ErasedSession: Send + Sync {
    fn home_dc_id(&self) -> Result<i32, ErasedSessionError>;

    fn set_home_dc_id(&self, dc_id: i32) -> BoxFuture<'_, Result<(), ErasedSessionError>>;

    fn dc_option(&self, dc_id: i32) -> Result<Option<DcOption>, ErasedSessionError>;

    fn set_dc_option(&self, dc_option: &DcOption) -> BoxFuture<'_, Result<(), ErasedSessionError>>;

    fn peer(&self, peer: PeerId) -> BoxFuture<'_, Result<Option<PeerInfo>, ErasedSessionError>>;

    fn peer_ref(&self, peer: PeerId) -> BoxFuture<'_, Result<Option<PeerRef>, ErasedSessionError>>;

    fn cache_peer(&self, peer: &PeerInfo) -> BoxFuture<'_, Result<(), ErasedSessionError>>;

    fn updates_state(&self) -> BoxFuture<'_, Result<UpdatesState, ErasedSessionError>>;

    fn set_update_state(
        &self,
        update: UpdateState,
    ) -> BoxFuture<'_, Result<(), ErasedSessionError>>;
}

impl<T: Session> ErasedSession for T {
    fn home_dc_id(&self) -> Result<i32, ErasedSessionError> {
        self.home_dc_id().map_err(|e| ErasedSessionError::from(e))
    }

    fn set_home_dc_id(&self, dc_id: i32) -> BoxFuture<'_, Result<(), ErasedSessionError>> {
        Box::pin(async move {
            self.set_home_dc_id(dc_id)
                .await
                .map_err(|e| ErasedSessionError::from(e))
        })
    }

    fn dc_option(&self, dc_id: i32) -> Result<Option<DcOption>, ErasedSessionError> {
        self.dc_option(dc_id)
            .map_err(|e| ErasedSessionError::from(e))
    }

    fn set_dc_option(&self, dc_option: &DcOption) -> BoxFuture<'_, Result<(), ErasedSessionError>> {
        let dc_option = dc_option.clone();
        Box::pin(async move {
            self.set_dc_option(&dc_option)
                .await
                .map_err(|e| ErasedSessionError::from(e))
        })
    }

    fn peer(&self, peer: PeerId) -> BoxFuture<'_, Result<Option<PeerInfo>, ErasedSessionError>> {
        Box::pin(async move {
            self.peer(peer)
                .await
                .map_err(|e| ErasedSessionError::from(e))
        })
    }

    fn peer_ref(&self, peer: PeerId) -> BoxFuture<'_, Result<Option<PeerRef>, ErasedSessionError>> {
        Box::pin(async move {
            self.peer_ref(peer)
                .await
                .map_err(|e| ErasedSessionError::from(e))
        })
    }

    fn cache_peer(&self, peer: &PeerInfo) -> BoxFuture<'_, Result<(), ErasedSessionError>> {
        let peer = peer.clone();
        Box::pin(async move {
            self.cache_peer(&peer)
                .await
                .map_err(|e| ErasedSessionError::from(e))
        })
    }

    fn updates_state(&self) -> BoxFuture<'_, Result<UpdatesState, ErasedSessionError>> {
        Box::pin(async {
            self.updates_state()
                .await
                .map_err(|e| ErasedSessionError::from(e))
        })
    }

    fn set_update_state(
        &self,
        update: UpdateState,
    ) -> BoxFuture<'_, Result<(), ErasedSessionError>> {
        Box::pin(async {
            self.set_update_state(update)
                .await
                .map_err(|e| ErasedSessionError::from(e))
        })
    }
}
